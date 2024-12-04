import asyncio
import time
import gc
from enum import Enum
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor  # Change this import

from utils import preprocess_csv_file
from jetstream import jetstream_ingest


import polars as pl
from alive_progress import alive_bar
import typer


class Partition(Enum):
    SINGLE = "single"
    EXCHANGE = "exchange"
    MULTI = "multi"

app = typer.Typer()


def process_partition(df, exchange_id):
    asyncio.run(jetstream_ingest(df, exchange_id))
    return exchange_id


@app.command()
def ingest(
    files: list[str],
    entity: str | None = None,
    partition: Partition = Partition.SINGLE,
    consumer_count: int = 1,
):
    total_message_count = 0


    def single_consumer(file: str, entity: str | None, consumer_count: int | None = 1):
        if consumer_count and consumer_count > 1:
            df = preprocess_csv_file(file)
            # Split dataframes by number of consumers
            ingesters: dict[int, pl.DataFrame] = dict.fromkeys(
                range(consumer_count), pl.DataFrame()
            )
            print(f"Pre processing into {consumer_count} partitions")
            with alive_bar(total=consumer_count) as bar:
                for i in range(consumer_count):
                    ingesters[i] = df.filter(pl.col("ID").hash(42) % consumer_count == i)
                    bar()
            del df
            gc.collect()

            message_count = 0

            start = time.time()
            with ProcessPoolExecutor(max_workers=consumer_count) as executor:
                futures = []
                for id, df in list(ingesters.items()):
                    print(f"Spawning task for {id} - ingesting {len(df)} events")
                    message_count += len(df)
                    future = executor.submit(process_partition, df, "exchange")
                    futures.append(future)
                print(f"Sending {message_count} message")
                with alive_bar(len(futures)) as bar:
                    for f in concurrent.futures.as_completed(futures):
                        _ = f.result()
                        bar()

            end = time.time()
            time_taken = round(end - start, 2)
            print(
                f"Sent {message_count} messages took {time_taken} seconds, {round(message_count / time_taken, 2)} message/s"
            )
            nonlocal total_message_count
            total_message_count += message_count
            print(f"Total messages sent: {total_message_count}")
        else:
            print("Starting ingestion into NATS server")
            df = preprocess_csv_file(file, entity=entity)
            asyncio.run(jetstream_ingest(df, "exchange", show_progress_bar=True))

    def exchange_consumer(file: str):
        df = preprocess_csv_file(file)
        exchanges = {}
        print("Splitting dataframe by exchange...")
        exchanges["ETR"] = df.filter(pl.col("ID").str.ends_with("ETR"))
        exchanges["FR"] = df.filter(pl.col("ID").str.ends_with("FR"))
        exchanges["NL"] = df.filter(pl.col("ID").str.ends_with("NL"))
        del df
        gc.collect()

        message_count = 0
        start = time.time()
        with ProcessPoolExecutor(max_workers=len(exchanges)) as executor:
            futures = []
            for id, df in list(exchanges.items()):
                print(f"Spawning task for {id} - ingesting {len(df)} events")
                message_count += len(df)
                future = executor.submit(process_partition, df, f"exchange.{id}")
                futures.append(future)
            print(f"Sending {message_count} message")
            with alive_bar(len(futures)) as bar:
                for f in concurrent.futures.as_completed(futures):
                    _ = f.result()
                    bar()
        end = time.time()
        time_taken = round(end - start, 2)

        print(
            f"Sent {message_count} messages took {time_taken} seconds, {round(message_count / time_taken, 2)} message/s"
        )

        nonlocal total_message_count
        total_message_count += message_count
        print(f"Total messages sent: {total_message_count}")

    def multi_consumer(file: str, consumer_count: int):
        df = preprocess_csv_file(file)
        # Split dataframes by number of consumers
        # Helping with typing issues
        ingesters: dict[int, pl.DataFrame] = dict.fromkeys(
            range(consumer_count), pl.DataFrame()
        )

        print(f"Pre processing into {consumer_count} partitions")
        with alive_bar(total=consumer_count) as bar:
            for i in range(consumer_count):
                ingesters[i] = df.filter(pl.col("ID").hash(42) % consumer_count == i)
                bar()
        del df
        gc.collect()

        message_count = 0

        start = time.time()
        with ProcessPoolExecutor(max_workers=consumer_count) as executor:
            futures = []
            for id, df in list(ingesters.items()):
                print(f"Spawning task for {id} - ingesting {len(df)} events")
                message_count += len(df)
                future = executor.submit(process_partition, df, f"exchange.{id}")
                futures.append(future)
            print(f"Sending {message_count} message")
            with alive_bar(len(futures)) as bar:
                for f in concurrent.futures.as_completed(futures):
                    _ = f.result()
                    bar()

        end = time.time()
        time_taken = round(end - start, 2)
        print(
            f"Sent {message_count} messages took {time_taken} seconds, {round(message_count / time_taken, 2)} message/s"
        )
        nonlocal total_message_count
        total_message_count += message_count
        print(f"Total messages sent: {total_message_count}")

    if consumer_count > 5504:
        raise ValueError(
            "consumer_count cannot exceed the number of exchanges (5504)"
        )
    # Run the async function
    for file in files:
        start = time.time()
        if partition == Partition.SINGLE:
            print(f"Running against single consumer, {consumer_count} tasks will be spawned")
            single_consumer(file, entity=entity, consumer_count=consumer_count)
        elif partition == Partition.EXCHANGE:
            print("Running 3 producers, 3 tasks will be created: [ETR, FR, NL]")
            exchange_consumer(file)
        elif partition == Partition.MULTI:
            print(f"Running as {consumer_count} ingesters")
            multi_consumer(file, consumer_count)
        else:
            raise ValueError("Invalid ingestion_mode")
        end = time.time()
        print(f"It took {round(end - start, 2)} seconds to process {file}")


if __name__ == "__main__":
    app()
