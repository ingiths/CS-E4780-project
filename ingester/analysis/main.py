from io import StringIO
from urllib.parse import urlencode
import polars as pl
import requests
import typer
import datetime
import pathlib
from alive_progress import alive_bar

# Print all rows by default
pl.Config.set_tbl_rows(-1)

app = typer.Typer()


def process_influx_data(csv_data: str) -> pl.DataFrame:
    # Skip the first row which contains the empty result field
    df = pl.read_csv(
        StringIO(csv_data),
        null_values=[""],
    )

    # Select only the columns we need
    df = df.select(
        [
            pl.col("_time").cast(pl.Datetime).dt.time().alias("time"),
            pl.col("_value").ceil().cast(pl.Int64).alias("value"),
            pl.col("_field").alias("field"),
        ]
    )

    # Pivot the data
    pivoted_df = df.pivot(
        "field", values="value", index="time", aggregate_function="first"
    )

    # Sort by time
    pivoted_df = pivoted_df.sort("time")

    return pivoted_df


def load_our_solution(entity: str, date: str):
    query_api_url = "http://localhost:8086/api/v2/query"
    headers = {
        "Authorization": "Token token",
        "Accept": "application/csv",
        "Content-type": "application/vnd.flux",
    }
    params = {"org": "trading-org"}
    flux_query = (
        'from(bucket: "trading_bucket")'
        f'|> range(start: time(v: "{date}T00:00:00Z"), stop: time(v: "{date}T23:59:59Z")) '
        '|> filter(fn: (r) => r["_measurement"] == "trading_bucket")'
        '|> filter(fn: (r) => r["_field"] == "first" or r["_field"] == "last" or r["_field"] == "max" or r["_field"] == "min" or r["_field"] == "movements")'
        f'|> filter(fn: (r) => r["id"] == "{entity}")'
        '|> keep(columns: ["_time", "_value", "_field"])'
        '|> drop(columns: ["result", "table", "_measurement", "equity_type", "id"])'
        '|> yield(name: "last")'
    )

    try:
        response = requests.post(
            f"{query_api_url}?{urlencode(params)}", headers=headers, data=flux_query
        )

        if response.status_code != 200:
            error_message = response.text
            raise Exception(
                f"InfluxDB Error (Status {response.status_code}): {error_message}"
            )

        # https://github.com/influxdata/influxdb/issues/6415
        # Shit from Influx is fucked, why add a , to the first character in the line!
        csv_data = [x.strip()[1:] for x in response.text.split("\n")]
        csv_data = [
            x for x in csv_data if x != "" and x != "result,table,_time,_value,_field"
        ]
        csv_data.insert(0, "table,trash,_time,_value,_field")
        return process_influx_data("\n".join(csv_data))

    except requests.exceptions.RequestException as e:
        raise Exception(f"Error querying InfluxDB: {str(e)}")


def load_actual_solution(file: str, entity: str) -> pl.DataFrame:
    df = (
        pl.scan_csv(
            file,
            separator=",",
            comment_prefix="#",
        )
        .select("ID", "Last", "Trading time")
        .filter(pl.col("ID") == entity)
        .collect()
    )
    df = df.drop_nulls()
    # Sometimes, the data is messed up
    df = df.with_columns([pl.col("Last").cast(pl.Float32)])
    df = df.with_columns(
        [
            pl.col("Trading time")
            .str.strptime(pl.Datetime, format="%H:%M:%S.%3f")
            .alias("Trading time")
        ]
    )[1:]
    # First row is always midnight, also skip some invalid events
    # The consumer drops them
    df = df.with_columns(
        [pl.col("Trading time").dt.truncate("5m").cast(pl.Time).alias("window_start")]
    ).filter(pl.col("Trading time").dt.time() >= pl.time(9, 0, 0))

    result = (
        df.group_by(["ID", "window_start"])
        .agg(
            [
                pl.col("Last").first().ceil().cast(pl.Int64).alias("First"),
                pl.col("Last").last().ceil().cast(pl.Int64).alias("Last"),
                pl.col("Last").max().ceil().cast(pl.Int64).alias("Max"),
                pl.col("Last").min().ceil().cast(pl.Int64).alias("Min"),
                pl.col("Last").len().alias("Movements"),
                pl.col("Trading time").last().cast(pl.Time).alias("Last update"),
                pl.col("Trading time").first().cast(pl.Time).alias("First update"),
            ]
        )
        .sort(["ID", "window_start"])
    )

    return result


@app.command()
def numerical(entity: str, date: str):
    print(load_our_solution(entity, date))


@app.command()
def analytical(data_file: str, entity: str):
    print(load_actual_solution(data_file, entity))


@app.command()
def compare(data_file: str, entity: str):
    file_trading_date = str(
        datetime.datetime.strptime(data_file[32:40], "%d-%m-%y").date()
    )
    our = load_our_solution(entity, file_trading_date)
    actual = load_actual_solution(data_file, entity)
    actual = actual.drop("Last update")
    # TODO: Empty windows
    print(f"Running comparison for {file_trading_date}")
    difference = pl.DataFrame().with_columns(
        [
            pl.Series(name="Time", values=our["time"]),
            pl.Series(name="First", values=our["first"] - actual["First"]),
            pl.Series(name="Last", values=our["last"] - actual["Last"]),
            pl.Series(name="Max", values=our["max"] - actual["Max"]),
            pl.Series(name="Min", values=our["min"] - actual["Min"]),
            pl.Series(name="Movements", values=our["movements"] - actual["Movements"]),
        ]
    )
    print(difference)


@app.command()
def print_event_count(limit: int = 10):
    previous = None
    paths = sorted(pathlib.Path("../data").glob("*.csv"))
    reversed(paths)
    with alive_bar(len(paths)) as bar:
        for path in sorted(pathlib.Path("../data").glob("*.csv")):
            counts = (
                pl.scan_csv(path, separator=",", comment_prefix="#")
                .select("ID")
                .group_by("ID")
                .len("Event Count")
                .collect()
            )
            if previous is None:
                previous = counts
            else:
                previous = previous.join(counts, on="ID").select(
                    pl.col("ID"), pl.col("Event Count") + pl.col("Event Count_right")
                )
            bar()
    if previous is not None:
        print(previous.sort(by="Event Count", descending=True).head(limit))
        print(f"Total events = {previous.sum()}")


@app.command()
def print_event_count_window(window: str = "5m", limit: int = 5):
    previous = None
    paths = sorted(pathlib.Path("../data").glob("*.csv"))[:5]
    reversed(paths)
    with alive_bar(len(paths)) as bar:
        for path in paths:
            df = (
                pl.scan_csv(path, separator=",", comment_prefix="#")
                .select("ID", "Trading time")
                .collect()
                .drop_nulls()
            )
            df = df.with_columns(
                [
                    pl.col("Trading time")
                    .str.strptime(pl.Datetime, format="%H:%M:%S.%3f")
                    .alias("Trading time")
                ]
            )[1:]

            df = df.with_columns(
                [
                    pl.col("Trading time")
                    .dt.truncate(window)
                    .cast(pl.Time)
                    .alias("window_start")
                    .cast(pl.Time)
                ]
            )
            df = df.group_by("window_start").len("Events in window")
            if previous is None:
                previous = df
            else:
                previous = previous.join(df, on="window_start").select(
                    pl.col("window_start"),
                    pl.col("Events in window") + pl.col("Events in window_right"),
                )
            bar()

    if previous is not None:
        print(previous.sort("Events in window", descending=True).head(limit))


if __name__ == "__main__":
    app()
