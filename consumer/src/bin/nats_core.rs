use anyhow::{anyhow, Result};
use bincode;
use clap::Parser;
use futures::StreamExt;
use influxdb::Client;
use tokio::{sync::mpsc, task::JoinSet};

use consumer::breakout::{self, BreakoutMessage};
use consumer::cli::{Cli, PartitionSubcommand};
use consumer::influx::{self, InfluxConfig, InfluxResults};
use consumer::tick::TickEvent;
use consumer::window;

async fn listen<T: AsRef<str>>(
    exchange: T,
    nats_client: async_nats::Client,
    influx_config: InfluxConfig,
) -> Result<()> {
    let (breakout_tx, breakout_rx) = mpsc::channel::<BreakoutMessage>(1000);
    let (influx_tx, influx_rx) = mpsc::channel::<InfluxResults>(1000);

    println!("Starting breakout Jestream producer");
    tokio::spawn(async move {
        breakout::start_core_nats_breakout_producer(breakout_rx).await;
    });
    let influx_client = Client::new("http://localhost:8086", "trading_bucket").with_token("token");
    println!(
        "Connected to InfluxDB server: name={} url={}",
        influx_client.database_name(),
        influx_client.database_url()
    );
    println!("Influx background batch writer starting");
    tokio::spawn(async move {
        influx::start_influx_background_writer(influx_client, influx_rx, influx_config).await;
    });

    let mut subscriber = nats_client
        .subscribe(exchange.as_ref().to_string())
        .await
        .map_err(|_| anyhow!("Could not subscribe to 'event"))?;
    println!("Subscribed to {}", exchange.as_ref());

    let mut manager = window::WindowManager::new();

    while let Some(message) = subscriber.next().await {
        let tick_event = bincode::deserialize::<TickEvent>(&message.payload)?;
        if !tick_event.is_valid() {
            continue;
        }
        if let Some(update) = manager.update(tick_event) {
            if let Some(b) = update.breakout.clone() {
                let breakout_message = BreakoutMessage::new(b.id, b.time, b.tags);
                breakout_tx.send(breakout_message).await?;
            }
            influx_tx.send(update).await?;
            {}
        }
    }

    Ok(())
}

async fn consumer<T: AsRef<str>>(exchange: T, config: InfluxConfig) -> Result<()> {
    let nats_client = async_nats::connect("localhost:4222").await.map_err(|_| {
        anyhow!("Could not connect to NATS server at localhost:4222, is the server running?")
    })?;
    println!("Connected to NATS server");
    listen(exchange, nats_client, config).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    println!(
        r"
    _   _    _  _____ ____  
    | \ | |  / \|_   _/ ___| 
    |  \| | / _ \ | | \___ \ 
    | |\  |/ ___ \| |  ___) |
    |_| \_/_/   \_\_| |____/ 
"
    );

    let influx_config = InfluxConfig::new(cli.batch_size, cli.flush_period);
    println!("Influx batch size = {}", influx_config.batch_size);
    println!("Influx flush period = {} ms", influx_config.flush_period);

    match cli.partition_subcommand {
        PartitionSubcommand::Single => {
            println!("Spawning single global consumer subscribed to the 'exchange' subject");
            consumer("exchange", influx_config).await?
        }
        PartitionSubcommand::ByExchange => {
            // Returns three results, when the futures never return
            println!("Spawning three exchange consumer subscribed to the 'exchange.FR', 'exchange.NL', 'exchange.ETR' subjects");
            let config_a = influx_config.clone();
            let config_b = influx_config.clone();
            let config_c = influx_config.clone();
            let (_, _, _) = tokio::join!(
                tokio::spawn(async move { consumer("exchange.FR", config_a).await }),
                tokio::spawn(async move { consumer("exchange.NL", config_b).await }),
                tokio::spawn(async move { consumer("exchange.ETR", config_c).await }),
            );
        }
        PartitionSubcommand::Multi { n } => {
            let mut set = JoinSet::new();
            for i in 0..n {
                println!(
                    "Spawning consumer subscribed to the 'exchange.{}' subject",
                    i
                );
                let config = influx_config.clone();
                set.spawn(async move { consumer(format!("exchange.{}", i), config.clone()).await });
            }
            // Wait forever...
            while let Some(_) = set.join_next().await {}
        }
    }

    Ok(())
}
