# CS-E4780-project

Scalable Systems and Data Management Course Project Detecting Trading Trends in Financial Tick Data

# Structure

- `docker/` contains a `docker-compose.yml` file to run the [NATS](https://nats.io/) server
- `ingester/` contains the source code for ingesting financial tick data into the NATS server
- `consumer/` contains the consumer of the NATS/JetStream subjects and calculates the quantative queries and breakout patterns.
- `data/` contains the `csv` file from the DEBS 2022 challenge.

> [!IMPORTANT]  
> The data from the DEBS challenge must reside in the `data/` directory.

# How to run

It is recommened to install the [`just` command runner](https://github.com/casey/just) and run the recipes specified in the `justfile`

Start by getting the available recipes by running `just -l`

```bash
$ just -l
Available recipes:
    consume mode partition batch-size="500" flush-period="500" count="1" # Start a consumer that listens for messages related to tick data
    c mode partition batch-size="500" flush-period="500" count="1" # alias for `consume`
    ingest-by-exchange mode                     # Start three ingesters/producer that partition the data and send it to the streams [exchange.ETR, exchange.FR, exchange.NL]
    ingest-multi mode count="1"                 # Start N ingesters/producer that partition the data and send it to the streams exchange.0 ... exchange.N - 1
    ingest-single mode consumer-count entity="" # Start some number of ingesters that send messages to a single subject named `exchange`
    profile program rate="1"                    # Profile CPU and memory usage of a program
    start mode                                  # Starts either a Core NATS server or a JetStream server
    s mode                                      # alias for `start`
```

## Start the Core NATS or JetStream

- `just start core` to start the Core NATS server
- `just start jetstream` to start the JetStream server

## Start the consumer

- `just consume core single` to start a single consumer that uses Core NATS streams 
- `just consume jetstream single` to start a single consumer that uses JetStream streams 
- `just consume core multi 500 500 5` to start five consumers, each subscribing to a different stream, with a influx batch size of 500 and a Influx flush period of 500 ms
- `just consume jetstream exchange` to start three consumers, partitioned by exchange IDs [ETR, FR, NL], each using JetStream streams

## Start the ingester/producer

- `just ingest-single core 1` start a single producer/ingester that sends events to the Core NATS server
- `just ingest-single core 1 ALE15.FR` start a single producer/ingester that sends messages only related to the ALE15.FR identifier to the Core NATS server
- `just ingest-single jetstream 1` start a single producer/ingester that sends events to the JetStream server
- `just ingest-by-exchange core` start a three producer/ingester, partitioned by exchange IDs [ETR, FR, NL] that sends events to the Core Nats server
- `just ingest-multi core` start a three producer/ingester, partitioned by exchange IDs [ETR, FR, NL] that sends events to the Core Nats server

## Start the breakout watcher

TODO

# Profiling

Profile using the `just profile <program> <file_name>` to acquire statistics about CPU and memory usage (uses `top` behind the scene) and output in a CSV format to `<file_name>`.

```bash
# Assume that a user has started a consumer with `just c core single`
just profile nats-core-consumer nats-core-single
# And the output will be in the `performance/nats-core-single.csv` file
```