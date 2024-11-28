use clap::{Parser, ValueEnum};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum NatsMode {
    Core,
    Jetstream,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum Partition {
    Global,
    ByExchange,
    Hash,
}

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[arg(value_enum)]
    pub nats_mode: NatsMode,
    #[arg(value_enum)]
    pub partition: Partition,
}
