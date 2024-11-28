mod cli;

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use bincode;
use chrono::{DateTime, TimeZone, Timelike, Utc};
use clap::Parser;
use futures::StreamExt;
use influxdb::{Client, InfluxDbWriteable};
use serde::{Deserialize, Serialize};

use cli::{Cli, NatsMode, Partition};

use async_nats::jetstream;
use async_nats::jetstream::consumer::PullConsumer;

const EMA_38: f32 = 38.0;
const EMA_100: f32 = 100.0;


fn round_down(number: i64, multiplier: i64) -> i64 { 
    number - (number % multiplier)
}

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
        // println!("{:#?}", self);

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
    time: DateTime<Utc>,
    btype: bool, // 0 for bull-ish, 1 for bear-ish
    #[influxdb(tag)]
    id: String,
}

impl Breakout {
    fn new(id: String, time: i64, btype: BreakoutType) -> Breakout {
        let time = Utc.timestamp_opt(time, 0).unwrap();
        let breakout = match btype {
            BreakoutType::Bullish => Breakout {
                id,
                time,
                btype: false,
            },
            BreakoutType::Bearish => Breakout {
                id,
                time,
                btype: true,
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
    previous: (f32, f32), // (ema_38, ema_100)
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
            previous: (0.0, 0.0),
            start_time,
            end_time: round_up(start_time, 300) - 300,
            first: price,
            last: 0.0,
            max: price,
            min: price,
        }
    }

    fn tumble(&mut self, new_start_time: i64, last_price: f32) -> Option<BreakoutType> {
        let current = self.ema.calc(last_price, self.previous);
        self.start_time = new_start_time;
        self.end_time = round_up(self.start_time,  300) - 300;
        self.sequence_number += 1;

        // Update values for candlestick chart
        self.first = last_price;
        self.last = 0.0; 
        self.max = last_price;
        self.min = last_price;

        let result = if current.0 < current.1 && self.previous.0 > self.previous.1 {
            Some(BreakoutType::Bearish)
        } else if current.0 > current.1 && self.previous.0 < self.previous.1 {
            Some(BreakoutType::Bullish)
        } else {
            None
        };

        self.previous = current;

        result
    }
}

struct TickEventManager {
    windows: HashMap<String, Window>,
    influx_client: influxdb::Client,
}

impl TickEventManager {
    fn new<T: AsRef<str>>(influx_url: T, influx_bucket: T) -> TickEventManager {
        let influx_client =
            Client::new(influx_url.as_ref(), influx_bucket.as_ref()).with_token("token");
        println!(
            "Connected to InfluxDB server: name={} url={}",
            influx_client.database_name(),
            influx_client.database_url()
        );
        TickEventManager {
            windows: HashMap::new(),
            influx_client,
        }
    }

    async fn update(&mut self, tick_event: TickEvent) {
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

        if window.end_time < trading_timestamp {
            // Write previous windows if the gap is jlarge
            // let empty_windows = ((trading_timestamp - window.end_time) as f32 / 300.0).ceil() as u32 - 1;

            // // Skip the last window (since it contains new values)
            // for _ in 0..empty_windows {
            //     let write_query = EmaResult {
            //         // Slow clone!?
            //         id: tick_event.id.clone(),
            //         calc_38: window.previous.0,
            //         calc_100: window.previous.1,
            //         time: Utc
            //             .timestamp_opt(round_down(tick_event.trading_timestamp.unwrap() as i64, 300), 0)
            //             .unwrap(),
            //         equity_type: tick_event.equity_type.clone(),
            //         first: window.last,
            //         last: window.last,
            //         max: window.last,
            //         min: window.last,
            //     }
            //     .into_query("trading_bucket");
            //     self.influx_client
            //         .query(write_query)
            //         .await
            //         .expect("Unable to write empty window");
            // }

            window.last = last;
            let window_first = window.first;
            let window_last = window.last;
            let window_max = window.max;
            let window_min = window.min;

            let breakout = window.tumble(trading_timestamp, tick_event.last.unwrap());
            let write_query = EmaResult {
                // Slow clone!?
                id: tick_event.id.clone(),
                calc_38: window.previous.0,
                calc_100: window.previous.1,
                time: Utc
                    .timestamp_opt(round_up(tick_event.trading_timestamp.unwrap() as i64, 300) - 300, 0)
                    .unwrap(),
                equity_type: tick_event.equity_type,
                first: window_first,
                last: window_last,
                max: window_max,
                min: window_min,
            }
            .into_query("trading_bucket");
            self.influx_client
                .query(write_query)
                .await
                .expect("Unable to write window");

            match breakout {
                Some(b) => {
                    let breakout = Breakout::new(tick_event.id, trading_timestamp as i64, b);
                    self.influx_client
                        .query(breakout.into_query("breakout"))
                        .await
                        .expect("Unable to write breakout event");
                }
                None => {}
            }

        }
    }
}

async fn start_core_nats_loop<T: AsRef<str>>(
    exchange: T,
    nats_client: async_nats::Client,
) -> Result<()> {
    let mut subscriber = nats_client
        .subscribe(exchange.as_ref().to_string())
        .await
        .map_err(|_| anyhow!("Could not subscribe to 'event"))?;
    println!("Subscribed to {}", exchange.as_ref());

    let mut manager = TickEventManager::new("http://localhost:8086", "trading_bucket");

    while let Some(message) = subscriber.next().await {
        let tick_event = bincode::deserialize::<TickEvent>(&message.payload)?;
        if !tick_event.is_valid() {
            // TODO: Log to error reporting service;
            continue;
        }
        manager.update(tick_event).await;
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

    let mut manager = TickEventManager::new("http://localhost:8086", "trading_bucket");

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

    println!("=== Config begin ===\npartition={:#?}\nnats_mode={:#?}\n=== Config end   ===\n", cli.partition, cli.nats_mode);

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
        Partition::Hash => { },
    }

    Ok(())
}
