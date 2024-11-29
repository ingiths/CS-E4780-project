import datetime
from enum import Enum
import asyncio
import nats
import struct
import zoneinfo
import pathlib
import time


import polars as pl
from alive_progress import alive_bar
import typer
from pydantic import BaseModel, Field
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# https://sentry.io/answers/print-colored-text-to-terminal-with-python/
BLACK = "\033[30m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"  # orange on some systems
BLUE = "\033[34m"
MAGENTA = "\033[35m"
CYAN = "\033[36m"
LIGHT_GRAY = "\033[37m"
DARK_GRAY = "\033[90m"
BRIGHT_RED = "\033[91m"
BRIGHT_GREEN = "\033[92m"
BRIGHT_YELLOW = "\033[93m"
BRIGHT_BLUE = "\033[94m"
BRIGHT_MAGENTA = "\033[95m"
BRIGHT_CYAN = "\033[96m"
WHITE = "\033[97m"

RESET = "\033[0m"  # called to return to standard terminal text color


class SecurityTypes(str, Enum):
    EQUITY = "E"
    INDEX = "I"


class Event(BaseModel):
    """
    Represents a financial market event with detailed pricing and trading information.
    The field order matches the corresponding CSV file structure - do not modify the order.
    """

    id: str = Field(description="Unique ID", validation_alias="ID")
    security_type: SecurityTypes = Field(
        description="Security Type (E)quity/(I)ndex", validation_alias="SecType"
    )
    last: float | None = Field(
        default=None, description="Last trade price", validation_alias="Last"
    )
    trading_time: datetime.time | None = Field(
        default=None,
        description="Time of last update (bid/ask/trade)",
        validation_alias="Trading time",
    )
    trading_date: datetime.date | None = Field(
        default=None, description="Date of last trade", validation_alias="Trading date"
    )

    def as_nats_message(self) -> bytes:
        if self.last is None:
            last_float = struct.pack("?", False)
        else:
            last_float = struct.pack("<?f", True, self.last)

        if self.trading_date and self.trading_time:
            # All event notifications are time-stamped with a global CEST timestamp in the format HH:MM:ss.ssss
            # https://en.wikipedia.org/wiki/Central_European_Summer_Time
            datetime_combined = datetime.datetime.combine(
                self.trading_date,
                self.trading_time,
                tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
            )
            unix = int(datetime_combined.strftime("%s"))
            # TODO: Finda  proper fix to CEST and UTC, not just adding 7200 seconds LOL
            # Avoid padding with <
            date_payload = struct.pack("<?I", True, unix + 2 * 3600)
        else:
            date_payload = struct.pack("?", False)

        id_bytes = self.id.encode("utf-8")
        sec_type_bytes = self.security_type.value.encode("utf-8")

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


@app.command()
def ingest(
    files: list[str], entity: str | None = None, flush_interval: int | None = None
):
    """
    Simulates event creation

    - How many events per second?
    - How many producers?
    """

    async def async_ingest():
        nc = await nats.connect("nats://localhost:4222")
        print(files)
        for file in files:
            file_trading_date = datetime.datetime.strptime(
                file[32:40], "%d-%m-%y"
            ).date()
            print(f"Reading file {file}")

            start = time.time()
            q = pl.scan_csv(file, comment_prefix="#", separator=",").select(
                "ID", "SecType", "Last", "Trading time"
            )
            if entity:
                q = q.filter(pl.col("ID") == entity)

            df = q.collect()
            df = df.with_columns(pl.lit(file_trading_date).alias("Trading date"))
            end = time.time()
            print(f"Read {file} in {round(end - start, 2)} seconds, shape: {df.shape}.")

            print("Starting ingestion into NATS server")
            with alive_bar(df.shape[0]) as bar:
                for i, row in enumerate(df.iter_rows(named=True)):
                    event = Event.model_validate(row)
                    exchange = event.id.split(".")[1]
                    # await nc.publish(f"exchange.{exchange}", event.as_nats_message())
                    await nc.publish("exchange", event.as_nats_message())

                    if flush_interval and i % flush_interval == 0:
                        await nc.flush()
                    bar()

        await nc.flush()
        await nc.close()

    asyncio.run(async_ingest())

@app.command()
def dummy():
    pass

if __name__ == "__main__":
    app()
