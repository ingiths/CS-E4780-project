use std::vec;

use anyhow::{anyhow, Result};
use async_nats::jetstream;
use bincode;
use clap::Parser;
use futures::StreamExt;
use influxdb::Client;
use tokio::{sync::mpsc, task::JoinSet};

use consumer::tick::TickEvent;
use consumer::window;
use consumer::breakout::{BreakoutMessage, self};
use consumer::cli::{Cli, PartitionSubcommand};
use consumer::influx::{InfluxConfig, InfluxResults, self};

async fn listen(
    js: jetstream::consumer::PullConsumer,
    influx_config: InfluxConfig,
) -> Result<()> {
    let (breakout_tx, breakout_rx) = mpsc::channel::<BreakoutMessage>(1000);
    let (influx_tx, influx_rx) = mpsc::channel::<InfluxResults>(1000);

    println!("Starting breakout Jestream producer");
    tokio::spawn(async move {
        breakout::start_breakout_producer(breakout_rx).await;
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

    let mut manager = window::WindowManager::new();
    // let mut messages = js.messages().await?;
    loop {
        // Sets max number of messages that can be buffered on the Client while processing already received messages. 
        // Higher values will yield better performance, but also potentially increase memory usage if application is acknowledging messages much slower than they arrive.
        if let Ok(message_stream) = js.stream().max_messages_per_batch(10_000).messages().await {
            let mut message_stream = message_stream.ready_chunks(10_000);
            while let Some(messages) = message_stream.next().await {
                if messages.is_empty() {
                    break;
                }
                for message in messages {
                    if let Ok(m) = message {
                        let f = m.ack();
                        let tick_event = bincode::deserialize::<TickEvent>(&m.payload)?;
                        if !tick_event.is_valid() {
                            f.await.unwrap();
                            continue;
                        }
                        if let Some(update) = manager.update(tick_event) {
                            if let Some(b) = update.breakout.clone() {
                                let breakout_message = BreakoutMessage::new(b.id, b.time, b.tags);
                                breakout_tx.send(breakout_message).await?;
                            }
                            influx_tx.send(update).await?;
                        }
                        f.await.unwrap();
                    }
                }
            }
        }
    }
}

async fn consumer<T: AsRef<str>>(exchange: T, influx_config: InfluxConfig) -> Result<()> {
    let nats_client = async_nats::connect("localhost:4222").await.map_err(|_| {
        anyhow!("Could not connect to NATS server at localhost:4222, is the server running?")
    })?;
    println!("Connected to NATS server");
    let jetstream = async_nats::jetstream::new(nats_client);

    println!("Listening to stream: 'trading-movements', subject: '{}'", exchange.as_ref().to_string());
    let consumer = jetstream
        .get_stream("trading-movements")
        .await?.create_consumer(jetstream::consumer::pull::Config {
            ack_policy: jetstream::consumer::AckPolicy::All,
            filter_subject: exchange.as_ref().to_string(),
            ..Default::default()
        }).await?;

    listen(consumer, influx_config).await?;
    Ok(())
}

async fn setup_stream(stream_name: impl Into<String>, subjects: Vec<impl Into<String>>) -> Result<()> {
    let nats_client = async_nats::connect("localhost:4222").await.map_err(|_| {
        anyhow!("Could not connect to NATS server at localhost:4222, is the server running?")
    })?;
    let stream_name = stream_name.into();

    let jetstream = async_nats::jetstream::new(nats_client);
    match jetstream.delete_stream(&stream_name).await {
        Ok(_) => println!("Deleted stream {} and setting up again", stream_name),
        Err(_) => {} 
    }
    let subjects = subjects.into_iter().map(|s| s.into()).collect();
    println!("Setting up subjects: {:#?}", subjects);
    jetstream
        .get_or_create_stream(jetstream::stream::Config {
            name: stream_name,
            subjects,
            ..Default::default()
        })
        .await?;

    Ok(())
}


#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    println!(r"
    _      _       _                            
    | | ___| |_ ___| |_ _ __ ___  __ _ _ __ ___  
 _  | |/ _ \ __/ __| __| '__/ _ \/ _` | '_ ` _ \ 
| |_| |  __/ |_\__ \ |_| | |  __/ (_| | | | | | |
 \___/ \___|\__|___/\__|_|  \___|\__,_|_| |_| |_|
");

    let influx_config = InfluxConfig::new(cli.batch_size, cli.flush_period);

    println!("Starting jetstream consumer");
    println!("Influx batch size = {}", influx_config.batch_size);
    println!("Influx flush period = {} ms", influx_config.flush_period);

    match cli.partition_subcommand {
        PartitionSubcommand::Single => {
            println!("Spawning single global consumer subscribed to the 'exchange' subject");
            setup_stream("trading-movements", vec!["exchange"]).await?;
            consumer("exchange", influx_config).await?
        }
        PartitionSubcommand::ByExchange => {
            // Returns three results, when the futures never return
            println!("Spawning three exchange consumer subscribed to the 'exchange.FR', 'exchange.NL', 'exchange.ETR' subjects");
            let config_1 = influx_config.clone();
            let config_2 = influx_config.clone();
            let config_3 = influx_config.clone();
            setup_stream("trading-movements", vec!["exchange.FR", "exchange.NL", "exchange.ETR"]).await?;
            let (_, _, _) = tokio::join!(
                tokio::spawn(async move { consumer("exchange.FR", config_1).await }),
                tokio::spawn(async move { consumer("exchange.NL", config_2).await }),
                tokio::spawn(async move { consumer("exchange.ETR", config_3).await }),
            );
        }
        PartitionSubcommand::Multi { n } => {
            println!("Spawning {} consumers", n);
            let subjects = (0..n).map(|i| format!("exchange.{}", i)).collect::<Vec<String>>();
            setup_stream("trading-movements", subjects).await?;
            let mut set = JoinSet::new();
            for i in 0..n {
                println!(
                    "Spawning consumer subscribed to the 'exchange.{}' subject",
                    i
                );
                let config = influx_config.clone();
                set.spawn(async move { consumer(format!("exchange.{}", i), config).await });
            }
            // Wait forever...
            while let Some(_) = set.join_next().await {}
        }
    }

    Ok(())
}
