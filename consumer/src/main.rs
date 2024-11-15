use std::collections::HashMap;

use anyhow::{anyhow, Result};
use bincode;
use chrono::{DateTime, TimeZone, Timelike, Utc};
use futures::StreamExt;
use influxdb::{Client, InfluxDbWriteable};
use serde::{Deserialize, Serialize};

const EMA_38: f32 = 38.0;
const EMA_100: f32 = 100.0;

#[derive(Serialize, Deserialize)]
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
    start_time: u32,
    end_time: u32,
}

impl Window {
    fn new(start_time: u32) -> Window {
        Window {
            ema: EMA::new(),
            sequence_number: 0,
            // (ema_38, ema_100)
            previous: (0.0, 0.0),
            start_time,
            end_time: start_time + 300,
        }
    }

    fn tumble(&mut self, new_start_time: u32, last_price: f32) -> Option<BreakoutType> {
        let current = self.ema.calc(last_price, self.previous);
        self.start_time = new_start_time;
        self.end_time = self.start_time + 300;
        self.sequence_number += 1;

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

async fn consumer<T: AsRef<str>>(exchange: T) -> Result<()> {
    let client = async_nats::connect("localhost:4222").await.map_err(|_| {
        anyhow!("Could not connect to NATS server at localhost:4222, is the server running?")
    })?;
    println!("Connected to NATS server");

    let mut subscriber = client
        .subscribe(exchange.as_ref().to_string())
        .await
        .map_err(|_| anyhow!("Could not subscribe to 'event"))?;
    println!("Subscribed to {}", exchange.as_ref());

    let client = Client::new("http://localhost:8086", "trading_bucket").with_token("token");
    println!(
        "Connected to InfluxDB server: name={} url={}",
        client.database_name(),
        client.database_url()
    );

    let mut windows = HashMap::new();

    while let Some(message) = subscriber.next().await {
        let tick_event = bincode::deserialize::<TickEvent>(&message.payload)?;

        if !tick_event.is_valid() {
            continue;
        }

        let trading_timestamp = tick_event
            .trading_timestamp
            .expect("Got invalid tick event");

        let window = windows
            .entry(tick_event.id.clone())
            .or_insert_with(|| Window::new(trading_timestamp));

        if window.end_time < trading_timestamp {
            let breakout = window.tumble(trading_timestamp, tick_event.last.unwrap());
            let write_query = EmaResult {
                // Slow clone!?
                id: tick_event.id.clone(),
                calc_38: window.previous.0,
                calc_100: window.previous.1,
                time: Utc
                    .timestamp_opt(tick_event.trading_timestamp.unwrap() as i64, 0)
                    .unwrap(),
                equity_type: tick_event.equity_type,
            }
            .into_query("trading_bucket");
            client.query(write_query).await?;

            match breakout {
                Some(b) => {
                    let breakout = Breakout::new(tick_event.id, trading_timestamp as i64, b);
                    client.query(breakout.into_query("breakout")).await?;
                }
                None => {}
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Returns three results, when the futures never return
    let (_, _, _) = tokio::join!(
        tokio::spawn(async move { consumer("exchange.FR".to_string()).await }),
        tokio::spawn(async move { consumer("exchange.NL".to_string()).await }),
        tokio::spawn(async move { consumer("exchange.ETR".to_string()).await }),
    );

    Ok(())
}
