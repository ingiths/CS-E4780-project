import datetime
import asyncio
import nats
import struct
import zoneinfo
import time
import gc
from enum import Enum
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor  # Change this import


import polars as pl
from alive_progress import alive_bar
import typer


class Partition(Enum):
    GLOBAL = "global"
    EXCHANGE = "exchange"
    COUNT = "count"


def create_nats_message(
    id: str, security_type: str, last: float, time: str, date: datetime.date
) -> bytes:
    if last is None:
        last_float = struct.pack("?", False)
    else:
        # Have to convert float for some reason - data is messed up sometimes
        last_float = struct.pack("<?f", True, float(last))

    if time and date:
        # All event notifications are time-stamped with a global CEST timestamp in the format HH:MM:ss.ssss
        # https://en.wikipedia.org/wiki/Central_European_Summer_Time
        datetime_combined = datetime.datetime.combine(
            date,
            datetime.datetime.strptime(time, "%H:%M:%S.%f").time(),
            tzinfo=zoneinfo.ZoneInfo("UTC"),
        )
        # Store millisecond precisison
        unix_ms = int(datetime_combined.timestamp() * 1000)
        # Avoid padding with <
        date_payload = struct.pack("<?Q", True, unix_ms)
    else:
        date_payload = struct.pack("?", False)

    id_bytes = id.encode("utf-8")
    sec_type_bytes = security_type.encode("utf-8")

    str_data = (
        struct.pack("I", len(id_bytes))
        + struct.pack("I", 0)
        + id_bytes
        + struct.pack("I", len(sec_type_bytes))
        + struct.pack("I", 0)
        + sec_type_bytes
    )

    # Must be aware of Rust memory layout to properly do this
    # https://doc.rust-lang.org/std/string/struct.String.html#representation
    return last_float + date_payload + str_data


app = typer.Typer()

async def nats_ingest(
    df: pl.DataFrame,
    exchange: str,
    flush_interval: int | None = 1000,
    show_progress_bar: bool = False,
):
    nc = await nats.connect("nats://localhost:4222")
    if show_progress_bar:
        with alive_bar(len(df)) as bar:
            for i, event in enumerate(df.iter_rows(named=False, buffer_size=1024)):
                await nc.publish(exchange, create_nats_message(*event))
                if flush_interval and i % flush_interval == 0:
                    await nc.flush()
                bar()
    else:
        for i, event in enumerate(df.iter_rows(named=False, buffer_size=1024)):
            await nc.publish(exchange, create_nats_message(*event))
            if flush_interval and i % flush_interval == 0:
                await nc.flush()

    await nc.flush()
    await nc.close()

def process_partition(df, exchange_id):
    asyncio.run(nats_ingest(df, exchange_id))
    return exchange_id

@app.command()
def ingest(
    files: list[str],
    entity: str | None = None,
    partition: Partition = Partition.GLOBAL,
    consumer_count: int | None = None
):
    total_message_count = 0

    def preprocess_csv_file(file: str, entity: str | None = None) -> pl.DataFrame:
        start = time.time()
        file_trading_date = datetime.datetime.strptime(file[32:40], "%d-%m-%y").date()
        print(f"Reading file {file}")

        q = pl.scan_csv(file, comment_prefix="#", separator=",").select(
            "ID", "SecType", "Last", "Trading time"
        )
        if entity:
            q = q.filter(pl.col("ID") == entity)

        df = q.collect()
        df = df.with_columns(pl.lit(file_trading_date).alias("Trading date"))
        end = time.time()
        print(f"Read {file} in {round(end - start, 2)} seconds, shape: {df.shape}.")
        return df


    async def global_ingest(file: str, entity: str | None):
        df = preprocess_csv_file(file, entity=entity)
        print("Starting ingestion into NATS server")
        await nats_ingest(df, "exchange", show_progress_bar=True)

    async def ingest_by_exchange(file: str):
        df = preprocess_csv_file(file)
        exchanges = {}
        print("Splitting dataframe by exchange...")
        exchanges['ETR'] = df.filter(pl.col("ID").str.ends_with("ETR"))
        exchanges['FR'] = df.filter(pl.col("ID").str.ends_with("FR"))
        exchanges['NL'] = df.filter(pl.col("ID").str.ends_with("NL"))
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
            for f in concurrent.futures.as_completed(futures):
                try:
                    process_id = f.result()
                    print(f"Process {process_id} completed successfully")
                except Exception as e:
                    print(f"Process failed with error: {e}")
        end = time.time()
        time_taken = round(end - start, 2)

        print(
            f"Sent {message_count} messages took {time_taken} seconds, {round(message_count / time_taken, 2)} message/s"
        )

        nonlocal total_message_count
        total_message_count += message_count
        print(f"Total messages sent: {total_message_count}")
    

    def ingest_by_id(file: str, consumer_count):
        df = preprocess_csv_file(file)
        # Split dataframes by number of consumers
        # Helping with typing issues
        ingesters: dict[int, pl.DataFrame] = dict.fromkeys(range(consumer_count), pl.DataFrame())

        print(f"Pre prcessing into {consumer_count} partitions")
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
            for f in concurrent.futures.as_completed(futures):
                try:
                    process_id = f.result()
                    print(f"Process {process_id} completed successfully")
                except Exception as e:
                    print(f"Process failed with error: {e}")


        end = time.time()
        time_taken = round(end - start, 2)
        print(
            f"Sent {message_count} messages took {time_taken} seconds, {round(message_count / time_taken, 2)} message/s"
        )
        nonlocal total_message_count
        total_message_count += message_count
        print(f"Total messages sent: {total_message_count}")

    # Run the async function
    for file in files:
        start = time.time()
        if partition == Partition.GLOBAL:
            print("Running single producer, no tasks will be spawned")
            asyncio.run(global_ingest(file, entity=entity))
        elif partition == Partition.EXCHANGE:
            print("Running 3 producers, 3 tasks will be created: [ETR, FR, NL]")
            asyncio.run(ingest_by_exchange(file))
        elif partition == Partition.COUNT:
            if not consumer_count:
                raise ValueError("consumer_count must not be None")
            print(f"Running as {consumer_count} ingesters")
            ingest_by_id(file, consumer_count)
        else:
            raise ValueError("Invalid ingestion_mode")
        end = time.time()
        print(f"It took {round(end - start, 2)} seconds to process {file}")


@app.command()
def dummy():
    pass


if __name__ == "__main__":
    app()
