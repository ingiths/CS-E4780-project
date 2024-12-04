import datetime
import zoneinfo
import struct
import time
import re

import polars as pl


def create_nats_message(
    id: str, security_type: str, last: float, time: str, date: datetime.date
) -> bytes:
    if last is None:
        last_float = struct.pack("?", False)
    else:
        # Have to convert float for some reason - data is messed up sometimes
        last_float = struct.pack("<?d", True, float(last))

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


def preprocess_csv_file(file: str, entity: str | None = None) -> pl.DataFrame:
    start = time.time()
    pattern = r".*debs\d{4}-gc-trading-day-(\d{2})-(\d{2})-(\d{2})\.csv"
    re_match = re.search(pattern, file)
    if not re_match:
        raise ValueError(f"No date found in supplied data file {file}")
    day, month, year = re_match.groups()
    print(f"Reading file {file}")
    date_str = f"20{year}-{month}-{day}"  # Assuming 20xx for the year
    dt = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()

    q = pl.scan_csv(file, comment_prefix="#", separator=",").select(
        "ID", "SecType", "Last", "Trading time"
    )
    if entity:
        q = q.filter(pl.col("ID") == entity)
    

    df = q.collect()
    df = df.with_columns(pl.lit(dt).alias("Trading date"))
    end = time.time()
    print(f"Read {file} in {round(end - start, 2)} seconds, shape: {df.shape}.")
    return df
