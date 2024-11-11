use std::collections::HashMap;
use std::fmt::{self};
use std::io::Write;

use anyhow::{anyhow, Result};
use bincode;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::AsyncWriteExt; // for write_all()

#[derive(Serialize, Deserialize)]
struct TickEvent {
    last: f32,
    trading_timestamp: Option<u32>,
    id: String,
    equity_type: String,
}

impl TickEvent {
    fn is_valid(&self) -> bool {
        // TODO: check if payloads are valid
        // TODO: Handle equity as well
        if self.trading_timestamp.is_none() || self.equity_type == "E" {
            return false;
        }
        true
    }
}

impl fmt::Display for TickEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TickEvent {{ \n  ID: {:?}\n  Type: {:?}\n  Last: ${:.2}\n  Trading timestamp: {:?} \n}}",
            self.id,
            self.equity_type,
            self.last,
            self.trading_timestamp,
        )
    }
}

struct EMACalculator {
    last: f32,
    window_duration: u32, // seconds
    smoothing_factor: f32,
    start: u32, // seconds
    prev: f32,
}

impl EMACalculator {
    fn new(window_duration: u32, smoothing_factor: f32) -> EMACalculator {
        EMACalculator {
            last: 0.0,
            window_duration,
            smoothing_factor,
            start: 0,
            prev: 0.0,
        }
    }

    fn update(&mut self, event: &TickEvent) -> bool {
        let trading_timestamp = event
            .trading_timestamp
            .expect(format!("Got invalid event: {}", event).as_str());
        if self.start == 0 {
            self.start = trading_timestamp;
        }
        // TODO: Less or equal OR only less
        if event.last != 0.0 {
            self.last = event.last;
        }

        // Check if window duration has exceeded, if so, calculate
        if self.start + self.window_duration < trading_timestamp {
            // New window starts here
            self.start = trading_timestamp;
            let calc = event.last * (2.0 / (1.0 + self.smoothing_factor))
                + self.prev * (1.0 - 2.0 / (1.0 + self.smoothing_factor));
            self.prev = calc;
            true
        } else {
            false
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = async_nats::connect("localhost:4222").await.map_err(|_| {
        anyhow!("Could not connect to NATS server at localhost:4222, is the server running?")
    })?;
    println!("Connected to NATS server");

    let mut subscriber = client
        .subscribe("event")
        .await
        .map_err(|_| anyhow!("Could not subscribe to 'event"))?;
    println!("Subscribed to events");

    let mut calculators = HashMap::new();

    let mut csv_file = File::create("results.csv").await?;
    csv_file
        .write_all("ID;Window;Last;Timestamp;Smoothing Factor;Calc\n".as_bytes())
        .await?;

    let mut valid_events = 0;
    let mut invalid_events = 0;
    while let Some(message) = subscriber.next().await {
        let tick_event = bincode::deserialize::<TickEvent>(&message.payload)?;

        if !tick_event.is_valid() {
            invalid_events += 1;
            if (valid_events + invalid_events) % 100 == 0 {
                print!(
                    "Valid events: {}. Invalid events: {} \r",
                    valid_events, invalid_events
                );
                std::io::stdout().flush().unwrap();
            }
            continue;
        } else {
            valid_events += 1;
            if (valid_events + invalid_events) % 100 == 0 {
                print!(
                    "Valid events: {}. Invalid events: {} \r",
                    valid_events, invalid_events
                );
                std::io::stdout().flush().unwrap();
            }
        }

        let emas = calculators.entry(tick_event.id.clone()).or_insert_with(|| {
            let ema_38 = EMACalculator::new(300, 38.0);
            let ema_100 = EMACalculator::new(300, 100.0);
            (ema_38, ema_100, 0)
        });

        let ema_38_updated = emas.0.update(&tick_event);
        let ema_100_updated = emas.1.update(&tick_event);

        if ema_38_updated && ema_100_updated {
            csv_file
                .write_all(
                    format!(
                        "{};{};{};{};{};{}\n",
                        tick_event.id,
                        emas.2,
                        tick_event.last,
                        tick_event.trading_timestamp.unwrap(),
                        38,
                        emas.0.prev
                    )
                    .as_bytes(),
                )
                .await?;
            csv_file
                .write_all(
                    format!(
                        "{};{};{};{};{};{}\n",
                        tick_event.id,
                        emas.2,
                        tick_event.last,
                        tick_event.trading_timestamp.unwrap(),
                        100,
                        emas.1.prev
                    )
                    .as_bytes(),
                )
                .await?;
            emas.2 += 1;
        }
    }

    Ok(())
}
