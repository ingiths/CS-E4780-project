use clap::{Parser, Subcommand};

#[derive(Clone, Debug, Subcommand)]
pub enum PartitionSubcommand {
    #[clap(about="Spawn a single global consumer")]
    Single,
    #[clap(about="Spawn a three 3 consumer, each listening to some exchange in [ETR, FR, NL]")]
    ByExchange,
    #[clap(about="Spawn N consumers, N > 0 and N < number of IDs")]
    Multi {
        #[arg(short)]
        n: usize
    }
}

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub partition_subcommand: PartitionSubcommand,
}
