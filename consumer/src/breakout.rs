use async_nats::jetstream::{self, stream};
use chrono::{DateTime, Utc};
use tokio::sync::mpsc::Receiver;

pub struct BreakoutMessage {
    id: String,
    ts: DateTime<Utc>,
    breakout_type: String,
}

impl BreakoutMessage {
    pub fn new<T: AsRef<str>>(id: T, ts: DateTime<Utc>, breakout_type: String) -> BreakoutMessage {
        BreakoutMessage {
            id: id.as_ref().to_string(),
            ts,
            breakout_type,
        }
    }
}

pub async fn start_core_nats_breakout_producer(mut receiver: Receiver<BreakoutMessage>) {
    let client = async_nats::connect("localhost:4222")
        .await
        .expect("Could not create NATS producer for breakout event");
    while let Some(breakout) = receiver.recv().await {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("ID", breakout.id.clone());
        match breakout.breakout_type.as_ref() {
            "bullish" => {
                client
                    .publish_with_headers(
                        "breakouts",
                        headers,
                        format!("Bullish event at {}", breakout.ts)
                            .bytes()
                            .collect(),
                    )
                    .await
                    .unwrap();
            }
            "bearish" => {
                client
                    .publish_with_headers(
                        "breakouts",
                        headers,
                        format!("Bearish event at {}", breakout.ts)
                            .bytes()
                            .collect(),
                    )
                    .await
                    .unwrap();
            }
            _ => panic!("This should not happen"),
        }
    }
}


pub async fn start_jetstream_breakout_producer(mut receiver: Receiver<BreakoutMessage>) {
    let client = async_nats::connect("localhost:4222")
        .await
        .expect("Could not create NATS producer for breakout event");
    let jetstream = async_nats::jetstream::new(client);
    let _ = jetstream.create_stream(jetstream::stream::Config {
        name: "breakout-events".to_string(),
        retention: stream::RetentionPolicy::Interest,
        subjects: vec!["breakouts.>".to_string()],
        ..Default::default()
    });
    while let Some(breakout) = receiver.recv().await {
        match breakout.breakout_type.as_ref() {
            "bullish" => {
                jetstream
                    .publish(
                        format!("breakouts.{}", breakout.id),
                        format!("Bullish event at {}", breakout.ts)
                            .bytes()
                            .collect(),
                    )
                    .await
                    .unwrap();
            }
            "bearish" => {
                jetstream
                    .publish(
                        format!("breakouts.{}", breakout.id),
                        format!("Bearish event at {}", breakout.ts)
                            .bytes()
                            .collect(),
                    )
                    .await
                    .unwrap();
            }
            _ => panic!("This should not happen"),
        }
    }
}
