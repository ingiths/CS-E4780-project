use clap::{Parser, Subcommand};

#[derive(Clone, Debug, Subcommand)]
pub enum PartitionSubcommand {
    #[clap(about = "One global consumer that listens for all events on the 'exchange' stream")]
    Single,
    #[clap(
        about = "Create three streams and one consumer per stream (exchange.ETR, exchange.FR, exchange.NL)"
    )]
    ByExchange,
    #[clap(about = "Create N consumers, listening on {exchange.0} ... {exchange.N - 1}")]
    Multi {
        #[arg(short)]
        n: usize,
    },
}

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub partition_subcommand: PartitionSubcommand,

    /// How big should the window batch size be before writing to influx.
    /// lower = more frequent results but higher load.
    /// higher = less frequent results, but less load
    #[arg(short, long, default_value_t = 500)]
    pub batch_size: usize,

    /// Flush period if no current batch size of windows does not exceed `batch_size`. Specified in milliseconds (ms)
    #[arg(short, long, default_value_t = 500)]
    pub flush_period: u64,
}
