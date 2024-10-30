use std::fmt::{self};

use bincode;
use chrono::DateTime;
use futures::StreamExt;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Payload {
    last: f32,
    trading_timestamp: Option<u32>,
    id: String,
    equity_type: String,
}

impl fmt::Display for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Convert Unix timestamps to readable dates
        println!("tmp {:?}", self.trading_timestamp);
        let trading_time = DateTime::from_timestamp(self.trading_timestamp.unwrap_or(0) as i64, 0)
            .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
        
        write!(
            f,
            "Payload {{ \n  ID: {:?}\n  Type: {:?}\n  Last: ${:.2}\n  Trading timestamp: {} \n}}",
            self.id,
            self.equity_type,
            self.last,
            trading_time.format("%Y-%m-%d %H:%M:%S")
        )
    }
}

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("localhost:4222").await?;

    let mut subscriber = client.subscribe("event").await?;

    let a = Some(1636322400);
    println!("Trading timestamp encoded {:?}", bincode::serialize(&a).unwrap());

    println!("Started NATS client");
    while let Some(message) = subscriber.next().await {
        // println!("Received message {:?}", message.payload);
        let tmp = bincode::deserialize::<Payload>(&message.payload);
        println!("{}", tmp.unwrap());
    }

    Ok(())
}
