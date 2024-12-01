mod cli;
mod influx;

use std::collections::HashMap;
use std::io::prelude::*;

use anyhow::{anyhow, Result};
use bincode;
use chrono::{DateTime, TimeZone, Timelike, Utc};
use clap::Parser;
use futures::StreamExt;
use influxdb::{Client, WriteQuery};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use cli::{Cli, NatsMode, Partition};

use tokio::time::timeout;

use influx::{BreakoutType, InfluxResults};

const EMA_38: f32 = 38.0;
const EMA_100: f32 = 100.0;

fn round_down(number: i64, multiplier: i64) -> i64 {
    ((number + multiplier/2) / multiplier) * multiplier
}

#[derive(Debug, Serialize, Deserialize)]
struct TickEvent {
    last: Option<f32>,
    trading_timestamp: Option<i64>,
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
                let trading_timestamp = Utc.timestamp_millis_opt(t).unwrap();
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
            end_time: start_time + 300 * 1000,
            first: price,
            last: 0.0,
            max: price,
            min: price,
        }
    }

    fn tumble(
        &mut self,
        new_start_time: i64,
        last_price: f32,
    ) -> Option<(BreakoutType, (f32, f32))> {
        // (ema_38, ema_100)
        println!("Tumbling, previous window was from {} - {}", DateTime::from_timestamp_millis(self.start_time).unwrap(), DateTime::from_timestamp_millis(self.end_time).unwrap());
        let (new_ema_38, new_ema_100) = self.ema.calc(last_price, self.current_emas);
        let (current_ema_38, current_ema_100) = self.current_emas;
        self.start_time = new_start_time;
        self.end_time = self.start_time + 300 * 1000;

        // Update values for candlestick chart
        self.first = last_price;
        self.last = 0.0;
        self.max = last_price;
        self.min = last_price;

        // A bearish breakout event occurs when:
        // - Current window: EMA_38 < EMA_100
        // - Previous window: EMA_38 >= EMA_100
        // Special case of event when windows start
        self.current_emas = (new_ema_38, new_ema_100);
        if self.sequence_number > 0 {
            let result = if new_ema_38 < new_ema_100 && current_ema_38 >= current_ema_100 {
                Some((BreakoutType::Bearish, (current_ema_38, current_ema_38)))
            } else if new_ema_38 > new_ema_100 && current_ema_38 <= current_ema_100 {
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

struct WindowManager {
    windows: HashMap<String, Window>,
}

impl WindowManager {
    fn new() -> WindowManager {
        WindowManager {
            windows: HashMap::new(),
        }
    }

    async fn update(&mut self, tick_event: TickEvent) -> Option<InfluxResults> {
        let trading_timestamp = tick_event
            .trading_timestamp
            .expect("Got invalid tick event");
        let last = tick_event.last.unwrap();


        let window = self
            .windows
            .entry(tick_event.id.clone())
            .or_insert_with(|| Window::new(round_down(trading_timestamp, 300 * 1000), last));

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

        // Perf event
        let start = Utc::now().timestamp_millis();

        window.last = last;
        let window_first = window.first;
        let window_last = window.last;
        let window_max = window.max;
        let window_min = window.min;

        let breakout = window.tumble(
            round_down(trading_timestamp as i64, 300 * 1000),
            last,
        );
        let mut result = InfluxResults::new(
            tick_event.id,
            round_down(trading_timestamp, 300 * 1000),
            tick_event.equity_type,
            window,
            window_first,
            window_last,
            window_max,
            window_min,
            breakout,
        );
        result.record_window_start(start);
        result.record_window_end(Utc::now().timestamp_millis());

        Some(result)
    }
}

async fn start_core_nats_loop<T: AsRef<str>>(
    exchange: T,
    nats_client: async_nats::Client,
) -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<InfluxResults>(1000);

    let influx_client = Client::new("http://localhost:8086", "trading_bucket").with_token("token");
    println!(
        "Connected to InfluxDB server: name={} url={}",
        influx_client.database_name(),
        influx_client.database_url()
    );

    tokio::spawn(async move {
        // TODO: Handle when buffer lenght is not 500
        let mut buffer = Vec::with_capacity(1000);
        let my_duration = tokio::time::Duration::from_millis(1000);
        loop {
            while let Ok(i) = timeout(my_duration, rx.recv()).await {
                if let Some(tmp) = i {
                    buffer.push(tmp);
                    if buffer.len() == 500 {
                        let mut window_writes = Vec::with_capacity(buffer.len());
                        let mut breakout_writes = Vec::with_capacity(buffer.len());
                        let mut perf_writes = Vec::with_capacity(buffer.len());
                        buffer.drain(..).into_iter().for_each(|influx_result| {
                            let (window_write, breakout_write, perf) = influx_result.into_query();
                            window_writes.push(window_write);
                            if let Some(bw) = breakout_write {
                                breakout_writes.push(bw);
                            }
                            perf_writes.push(perf);
                        });
                        influx_client.query(window_writes).await.unwrap();
                        influx_client.query(breakout_writes).await.unwrap();
                        let write_end = Utc::now().timestamp_millis();
                        let perf_writes = perf_writes
                            .into_iter()
                            .map(|mut p| {
                                // Not good design but not a lot of time left
                                p.influx_write_end = write_end;
                                p.into_influx_query()
                            })
                            .collect::<Vec<WriteQuery>>();
                        influx_client.query(perf_writes).await.unwrap();
                    }
                }
            }
            // Timeout of
            if !buffer.is_empty() {
                let mut window_writes = Vec::with_capacity(buffer.len());
                let mut breakout_writes = Vec::with_capacity(buffer.len());
                let mut perf_writes = Vec::with_capacity(buffer.len());
                buffer.drain(..).into_iter().for_each(|influx_result| {
                    let (window_write, breakout_write, perf) = influx_result.into_query();
                    window_writes.push(window_write);
                    if let Some(bw) = breakout_write {
                        breakout_writes.push(bw);
                    }
                    perf_writes.push(perf);
                });
                influx_client.query(window_writes).await.unwrap();
                influx_client.query(breakout_writes).await.unwrap();
                let write_end = Utc::now().timestamp_millis();
                let perf_writes = perf_writes
                    .into_iter()
                    .map(|mut p| {
                        // Not good design but not a lot of time left
                        p.influx_write_end = write_end;
                        p.into_influx_query()
                    })
                    .collect::<Vec<WriteQuery>>();
                influx_client.query(perf_writes).await.unwrap();
            }
            continue;
        }
    });

    let mut subscriber = nats_client
        .subscribe(exchange.as_ref().to_string())
        .await
        .map_err(|_| anyhow!("Could not subscribe to 'event"))?;
    println!("Subscribed to {}", exchange.as_ref());

    let mut manager = WindowManager::new();

    let mut counter = 0;

    while let Some(message) = subscriber.next().await {
        let tick_event = bincode::deserialize::<TickEvent>(&message.payload)?;
        counter += 1;
        if counter % 100 == 0 {
            print!("Counter: {}\r", counter);
            std::io::stdout().flush()?;
        }
        if !tick_event.is_valid() {
            continue;
        }
        if let Some(window) = manager.update(tick_event).await {
            tx.send(window).await?;
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
            unimplemented!();
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
