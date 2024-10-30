import csv
import datetime
from enum import Enum
from typing import Generator, Annotated
import asyncio
import nats
import struct
import zoneinfo


import typer
from pydantic import BaseModel, ConfigDict, field_validator, Field


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

    # Star
    id: str = Field(description="Unique ID")
    # Star
    security_type: SecurityTypes = Field(description="Security Type (E)quity/(I)ndex")
    # Star
    date: datetime.date | None = Field(
        default=None, description="System date for last received update"
    )
    # Star
    time: datetime.time | None = Field(
        default=None, description="System time for last received update"
    )
    ask: str = Field(description="Price of best ask order")
    ask_volume: str = Field(description="Volume of best ask order")
    bid: str = Field(description="Price of best bid order")
    bid_volume: str = Field(description="Volume of best bid order")
    ask_time: str = Field(description="Time of last ask")
    day_high_ask: str = Field(description="Day's high ask")
    close: str = Field(description="Closing price")
    currency: str = Field(description="Currency (according to ISO 4217)")
    day_high_ask_time: str = Field(description="Day's high ask time")
    day_high: str = Field(description="Day's high")
    isin: str = Field(
        description="ISIN (International Securities Identification Number)"
    )
    auction_price: str = Field(description="Price at midday's auction")
    day_low_ask: str = Field(description="Lowest ask price of the current day")
    day_low: str = Field(description="Lowest price of the current day")
    day_low_ask_time: str = Field(
        description="Time of lowest ask price of the current day"
    )
    open: str = Field(description="First price of current trading day")
    nominal_value: str = Field(description="Nominal Value")
    # Star
    last: float | None = Field(default=None, description="Last trade price")
    last_volume: str = Field(description="Last trade volume")
    # Star
    trading_time: datetime.time | None = Field(
        default=None, description="Time of last update (bid/ask/trade)"
    )
    total_volume: str = Field(description="Cumulative volume for current trading day")
    mid_price: str = Field(description="Mid price (between bid and ask)")
    # Star
    trading_date: datetime.date | None = Field(
        default=None, description="Date of last trade"
    )
    profit: str = Field(description="Profit")
    current_price: str = Field(description="Current price")
    related_indices: str = Field(description="Related indices")
    day_high_bid_time: str = Field(description="Days high bid time")
    day_low_bid_time: str = Field(description="Days low bid time")
    open_time: str = Field(description="Time of open price")
    last_price_time: str = Field(description="Time of last trade")
    close_time: str = Field(description="Time of closing price")
    day_high_time: str = Field(description="Time of days high")
    day_low_time: str = Field(description="Time of days low")
    bid_time: str = Field(description="Time of last bid update")
    auction_time: str = Field(description="Time when last auction price was made")

    model_config = ConfigDict(
        frozen=True,
    )

    @field_validator("date", mode="before")
    @classmethod
    def transform_date(cls, raw: str) -> datetime.date | None:
        if raw == "":
            return None
        return datetime.datetime.strptime(raw, "%d-%m-%Y").date()

    @field_validator("time", mode="before")
    @classmethod
    def transform_time(cls, raw: str) -> datetime.time | None:
        if raw == "" or raw == "::":
            return None
        return datetime.datetime.strptime(raw, "%H:%M:%S.%f").time()

    @field_validator("last", mode="before")
    @classmethod
    def transform_last(cls, raw: str) -> float | None:
        if raw == "" or raw == "::":
            return None
        return float(raw)

    @field_validator("trading_time", mode="before")
    @classmethod
    def transform_trading_time(cls, raw: str) -> datetime.time | None:
        if raw == "" or raw == "::":
            return None
        return datetime.datetime.strptime(raw, "%H:%M:%S.%f").time()

    @field_validator("trading_date", mode="before")
    @classmethod
    def transform_trading_date(cls, raw: str) -> datetime.date | None:
        if raw == "":
            return None
        return datetime.datetime.strptime(raw, "%d-%m-%Y").date()

    def __str__(self) -> str:
        if None not in [self.last, self.trading_time, self.trading_date]:
            return (
                f"{GREEN}Event(id={self.id}, "
                f"security_type={self.security_type.value}, "
                f"last={self.last}, "
                f"trading_time={self.trading_time}, "
                f"trading_date={self.trading_date}){RESET}"
            )
        else:
            return (
                f"{RED}Event(id={self.id}, "
                f"security_type={self.security_type.value}, "
                f"last={self.last}, "
                f"trading_time={self.trading_time}, "
                f"trading_date={self.trading_date}){RESET}"
            )

    def as_nats_message(self) -> bytes:
        last_float = struct.pack("f", self.last if self.last is not None else 0.0)

        if self.trading_date and self.trading_time:
            # All event notifications are time-stamped with a global CEST timestamp in the format HH:MM:ss.ssss
            # https://en.wikipedia.org/wiki/Central_European_Summer_Time
            datetime_combined = datetime.datetime.combine(self.trading_date, self.trading_time, tzinfo=zoneinfo.ZoneInfo("Europe/Paris"))
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


def read_tick_data(
    file: str, limit: int = -1, offset: int = 0
) -> Generator[Event, None, None]:
    event_count = 0
    with open(file, "r") as f:
        skip = 13 + offset
        for _ in range(skip):
            next(f)
        reader = csv.DictReader(
            f, delimiter=",", fieldnames=list(Event.model_fields.keys())
        )
        for row in reader:
            if event_count == limit:
                break
            yield Event.model_validate(row)
            event_count += 1


@app.command()
def ingest(
    files: list[str],
    limit: Annotated[int, typer.Option("-l")] = -1,
):
    """
    Simulates event creation

    - How many events per second?
    - How many producers?
    """

    async def async_ingest():
        nc = await nats.connect("nats://localhost:4222")
        js = nc.jetstream()

        await js.add_stream(name="tick", subjects=["event"])

        reader = read_tick_data(files[0])
        for i in range(limit):
            event = next(reader)
            print(event)
            await js.publish("event", event.as_nats_message())

        await nc.close()

    asyncio.run(async_ingest())


@app.command()
def explore(
    file: str,
    limit: Annotated[int, typer.Option("-l")] = -1,
    offset: Annotated[int, typer.Option("-o")] = 0,
    summarize: bool = True,
    print_event: bool = False,
):
    events = []
    event_stream = read_tick_data(file, limit, offset)
    for event in event_stream:
        if print_event:
            print(event)
        events.append(event)
    if summarize:
        if print_event:
            print("===")
        print(f"{BRIGHT_GREEN}Processed {len(events)} rows{RESET}")
        unique_ids = set([e.id for e in events])
        etr_exchange = set([e.id for e in events if e.id.split(".")[1] == "ETR"])
        fr_exchange = set([e.id for e in events if e.id.split(".")[1] == "FR"])
        nl_exchange = set([e.id for e in events if e.id.split(".")[1] == "NL"])
        print(f"{BRIGHT_GREEN}Unique IDs{RESET} {len(unique_ids)}")
        print(f"- {BRIGHT_GREEN}ETR{RESET} {len(etr_exchange)}")
        print(f"- {BRIGHT_GREEN}FR{RESET} {len(fr_exchange)}")
        print(f"- {BRIGHT_GREEN}NL{RESET} {len(nl_exchange)}")


if __name__ == "__main__":
    app()
