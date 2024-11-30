mod cli;

use std::collections::HashMap;
use std::io::prelude::*;

use anyhow::{anyhow, Result};
use bincode;
use chrono::{DateTime, TimeZone, Timelike, Utc};
use clap::Parser;
use futures::StreamExt;
use influxdb::{Client, InfluxDbWriteable, WriteQuery};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use cli::{Cli, NatsMode, Partition};

use async_nats::jetstream;
use async_nats::jetstream::consumer::PullConsumer;
use tokio::time::timeout;

const EMA_38: f32 = 38.0;
const EMA_100: f32 = 100.0;

fn round_up(number: i64, multiplier: i64) -> i64 {
    ((number + multiplier - 1) / multiplier * multiplier) as i64
}

#[derive(Debug, Serialize, Deserialize)]
struct TickEvent {
    last: Option<f32>,
    trading_timestamp: Option<u32>,
    id: String,
    equity_type: String,
}

impl TickEvent {
    fn is_valid(&self) -> bool {
        if self.last.is_none() || self.trading_timestamp.is_none() {
            return false;
        }

        let result = match self.trading_timestamp {
            Some(t) => {
                let trading_timestamp = Utc.timestamp_opt(t as i64, 0).unwrap();
                if trading_timestamp.hour() == 0
                    && trading_timestamp.minute() == 0
                    && trading_timestamp.second() == 0
                {
                    return false;
                }
                return true;
            }
            None => false,
        };

        result
    }
}

#[derive(InfluxDbWriteable)]
struct EmaResult {
    time: DateTime<Utc>,
    calc_38: f32,
    calc_100: f32,
    first: f32, // Price of window opening
    last: f32,  // Price of window closing
    max: f32,   // Max value of window
    min: f32,   // Min value of window
    #[influxdb(tag)]
    id: String,
    #[influxdb(tag)]
    equity_type: String,
}

#[derive(InfluxDbWriteable, Debug)]
struct Breakout {
    #[influxdb(tag)]
    id: String,
    #[influxdb(tag)]
    tags: String,
    time: DateTime<Utc>,
    title: String,
}

impl Breakout {
    fn new(
        id: String,
        time: i64,
        btype: BreakoutType,
        previous: (f32, f32),
        current: (f32, f32),
    ) -> Breakout {
        let time = Utc.timestamp_opt(time, 0).unwrap();
        let breakout = match btype {
            BreakoutType::Bullish => Breakout {
                id: id.clone(),
                time,
                title: format!("Bullish event (BUY BUY BUY!) for {} due to: WindowCurrent({} > {}) AND WindowPrevious({} <= {})", id, current.0, current.1, previous.0, previous.1),
                tags: "bullish".to_string(),
            },
            BreakoutType::Bearish => Breakout {
                id: id.clone(),
                time,
                title: format!("Bearish event (SELL SELL SELL!) for {} due to WindowCurrent({} < {}) AND WindowPrevious({} >= {})", id, current.0, current.1, previous.0, previous.1),
                tags: "bearish".to_string(),
            },
        };
        breakout
    }
}

struct EMA {
    ema_38: f32,
    ema_100: f32,
}

impl EMA {
    fn new() -> EMA {
        EMA {
            ema_38: 0.0,
            ema_100: 0.0,
        }
    }

    fn calc(&mut self, last_price: f32, previous: (f32, f32)) -> (f32, f32) {
        self.ema_38 =
            last_price * (2.0 / (1.0 + EMA_38)) + previous.0 * (1.0 - 2.0 / (1.0 + EMA_38));
        self.ema_100 =
            last_price * (2.0 / (1.0 + EMA_100)) + previous.1 * (1.0 - 2.0 / (1.0 + EMA_100));
        (self.ema_38, self.ema_100)
    }
}

enum BreakoutType {
    Bullish,
    Bearish,
}

struct Window {
    ema: EMA,
    sequence_number: u32,
    current_emas: (f32, f32), // (ema_38, ema_100)
    start_time: i64,
    end_time: i64,
    // Information about prices
    first: f32, // Price of window opening
    last: f32,  // Price of window closing
    max: f32,   // Max value of window
    min: f32,   // Min value of window
}

impl Window {
    fn new(start_time: i64, price: f32) -> Window {
        Window {
            ema: EMA::new(),
            sequence_number: 0,
            // (ema_38, ema_100)
            current_emas: (0.0, 0.0),
            start_time,
            end_time: round_up(start_time, 300) - 300,
            first: price,
            last: 0.0,
            max: price,
            min: price,
        }
    }

    fn tumble(
        &mut self,
        id: &String,
        new_start_time: i64,
        last_price: f32,
    ) -> Option<(BreakoutType, (f32, f32))> {
        // (ema_38, ema_100)
        let (new_ema_38, new_ema_100) = self.ema.calc(last_price, self.current_emas);
        let (current_ema_38, current_ema_100) = self.current_emas;
        self.start_time = new_start_time;
        self.end_time = self.start_time + 300;

        // Update values for candlestick chart
        self.first = last_price;
        self.last = 0.0;
        self.max = last_price;
        self.min = last_price;

        // A bearish breakout event occurs when:
        // - Curernt window: EMA_38 < EMA_100
        // - Previous window: EMA_38 >= EMA_100
        // Special case of event when windows start
        self.current_emas = (new_ema_38, new_ema_100);
        if self.sequence_number > 0 {
            let ts = Utc.timestamp_opt(new_start_time, 0).unwrap();
            let result = if new_ema_38 < new_ema_100 && current_ema_38 >= current_ema_100 {
                println!("[{}] {} Bearish because Current: [{} < {}] and Previous [{} >= {}]", id, ts, new_ema_38, new_ema_100, current_ema_38, current_ema_100);
                Some((BreakoutType::Bearish, (current_ema_38, current_ema_38)))
            } else if new_ema_38 > new_ema_100 && current_ema_38 <= current_ema_100 {
                println!("[{}] {} Bullish because Current: [{} > {}] and Previous [{} <= {}]", id, ts, new_ema_38, new_ema_100, current_ema_38, current_ema_100);
                Some((BreakoutType::Bullish, (current_ema_38, current_ema_100)))
            } else {
                None
            };
            self.sequence_number += 1;
            result
        } else {
            self.sequence_number += 1;
            None
        }


    }
}

struct TickEventManager {
    windows: HashMap<String, Window>,
}

impl TickEventManager {
    fn new() -> TickEventManager {
        TickEventManager {
            windows: HashMap::new(),
        }
    }

    async fn update(&mut self, tick_event: TickEvent) -> Option<(EmaResult, Option<Breakout>)> {
        let trading_timestamp = tick_event
            .trading_timestamp
            .expect("Got invalid tick event") as i64;
        let last = tick_event.last.unwrap();

        let window = self
            .windows
            .entry(tick_event.id.clone())
            .or_insert_with(|| Window::new(trading_timestamp, last));

        // Does not update when this window is created
        if window.max < last {
            window.max = last;
        }

        if last < window.min {
            window.min = last;
        }

        if window.end_time >= trading_timestamp {
            return None;
        }

        window.last = last;
        let window_first = window.first;
        let window_last = window.last;
        let window_max = window.max;
        let window_min = window.min;

        let breakout = window.tumble(
            &tick_event.id,
            round_up(trading_timestamp as i64, 300) - 300,
            last,
        );
        let ema_result = EmaResult {
            // Slow clone!?
            id: tick_event.id.clone(),
            calc_38: window.current_emas.0,
            calc_100: window.current_emas.1,
            time: Utc
                .timestamp_opt(round_up(trading_timestamp as i64, 300) - 300, 0)
                .unwrap(),
            equity_type: tick_event.equity_type,
            first: window_first,
            last: window_last,
            max: window_max,
            min: window_min,
        };
        if let Some(b) = breakout {
            let breakout = Breakout::new(
                tick_event.id,
                round_up(trading_timestamp as i64, 300) - 300,
                b.0,
                b.1,
                window.current_emas,
            );
            Some((ema_result, Some(breakout)))
        } else {
            Some((ema_result, None))
        }
    }
}

async fn start_core_nats_loop<T: AsRef<str>>(
    exchange: T,
    nats_client: async_nats::Client,
) -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<(EmaResult, Option<Breakout>)>(1000);

    let influx_client = Client::new("http://localhost:8086", "trading_bucket").with_token("token");
    println!(
        "Connected to InfluxDB server: name={} url={}",
        influx_client.database_name(),
        influx_client.database_url()
    );

    tokio::spawn(async move {
        // TODO: Handle when buffer lenght is not 500
        let mut buffer = Vec::with_capacity(500);
        let my_duration = tokio::time::Duration::from_millis(500);
        loop {
            while let Ok(i) = timeout(my_duration, rx.recv()).await {
                if let Some(tmp) = i {
                    buffer.push(tmp);
                    if buffer.len() == 500 {
                        let windows = buffer.drain(..);
                        let (windows, breakouts): (Vec<EmaResult>, Vec<Option<Breakout>>) =
                            windows.into_iter().unzip();
                        let windows = windows
                            .into_iter()
                            .map(|w| w.into_query("trading_bucket"))
                            .collect::<Vec<WriteQuery>>();
                        let breakouts = breakouts
                            .into_iter()
                            .filter(|b| b.is_some())
                            .map(|b| b.unwrap())
                            .map(|b| b.into_query("breakout"))
                            .collect::<Vec<WriteQuery>>();
                        influx_client.query(windows).await.unwrap();
                        influx_client.query(breakouts).await.unwrap();
                    }
                }
            }
            if !buffer.is_empty() {
                let windows = buffer.drain(..);
                let (windows, breakouts): (Vec<EmaResult>, Vec<Option<Breakout>>) =
                    windows.into_iter().unzip();
                let windows = windows
                    .into_iter()
                    .map(|w| w.into_query("trading_bucket"))
                    .collect::<Vec<WriteQuery>>();
                let breakouts = breakouts
                    .into_iter()
                    .filter(|b| b.is_some())
                    .map(|b| b.unwrap())
                    .map(|b| b.into_query("breakout"))
                    .collect::<Vec<WriteQuery>>();
                influx_client.query(windows).await.unwrap();
                influx_client.query(breakouts).await.unwrap();
            }
            continue;
        }
    });

    let mut subscriber = nats_client
        .subscribe(exchange.as_ref().to_string())
        .await
        .map_err(|_| anyhow!("Could not subscribe to 'event"))?;
    println!("Subscribed to {}", exchange.as_ref());

    let mut manager = TickEventManager::new();

    let mut counter = 0;

    while let Some(message) = subscriber.next().await {
        let tick_event = bincode::deserialize::<TickEvent>(&message.payload)?;
        counter += 1;
        if counter % 100 == 0 {
            print!("Counter: {}\r", counter);
            std::io::stdout().flush().unwrap();
        }
        if !tick_event.is_valid() {
            continue;
        }
        if let Some(window) = manager.update(tick_event).await {
            tx.send(window).await.unwrap();
        }
    }

    Ok(())
}

async fn start_jestream_loop<T: AsRef<str>>(
    exchange: T,
    nats_client: async_nats::Client,
) -> Result<()> {
    let jetstream = async_nats::jetstream::new(nats_client);
    println!("Created jetstream context");

    let stream_name = String::from("events");

    // Create a pull-based consumer
    let consumer: PullConsumer = jetstream
        .create_stream(jetstream::stream::Config {
            name: stream_name,
            subjects: vec![exchange.as_ref().to_string()],
            ..Default::default()
        })
        .await?
        .create_consumer(jetstream::consumer::pull::Config {
            // Setting durable_name to Some(...) will cause this consumer to be "durable".
            // This may be a good choice for workloads that benefit from the JetStream server or cluster remembering the progress of consumers for fault tolerance purposes.
            // If a consumer crashes, the JetStream server or cluster will remember which messages the consumer acknowledged.
            // When the consumer recovers, this information will allow the consumer to resume processing where it left off. If you're unsure, set this to Some(...).
            durable_name: Some("consumer".to_string()),
            ..Default::default()
        })
        .await?;
    println!("Created jetstream pull consumer");

    let mut messages = consumer.messages().await?;

    let mut manager = TickEventManager::new();

    while let Some(message) = messages.next().await {
        match message {
            Ok(m) => {
                let tick_event = bincode::deserialize::<TickEvent>(&m.payload)?;
                if !tick_event.is_valid() {
                    // TODO: Log to error reporting service;
                    continue;
                }
                manager.update(tick_event).await;
            }
            Err(_) => unimplemented!(),
        }
    }

    Ok(())
}

async fn consumer<T: AsRef<str>>(exchange: T, mode: NatsMode) -> Result<()> {
    let nats_client = async_nats::connect("localhost:4222").await.map_err(|_| {
        anyhow!("Could not connect to NATS server at localhost:4222, is the server running?")
    })?;
    println!("Connected to NATS server");

    match mode {
        NatsMode::Core => {
            start_core_nats_loop(exchange, nats_client).await?;
            Ok(())
        }
        NatsMode::Jetstream => {
            start_jestream_loop(exchange, nats_client).await?;
            Ok(())
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    println!(
        "=== Config begin ===\npartition={:#?}\nnats_mode={:#?}\n=== Config end   ===\n",
        cli.partition, cli.nats_mode
    );

    match cli.partition {
        Partition::Global => consumer("exchange", cli.nats_mode).await?,
        Partition::ByExchange => {
            // Returns three results, when the futures never return
            let (_, _, _) = tokio::join!(
                tokio::spawn(async move { consumer("exchange.FR", cli.nats_mode).await }),
                tokio::spawn(async move { consumer("exchange.NL", cli.nats_mode).await }),
                tokio::spawn(async move { consumer("exchange.ETR", cli.nats_mode).await }),
            );
        }
        Partition::Hash => {}
    }

    Ok(())
}
