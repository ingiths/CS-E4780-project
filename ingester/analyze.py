import polars as pl
import typer

app = typer.Typer()

@app.command()
def analyze(file: str, entity: str) -> pl.DataFrame:
    df = pl.scan_csv(
        file,
        separator=",",
        comment_prefix="#",
    ).select("ID", "Last", "Trading time").filter(pl.col("ID") == entity).collect()
    df = df.drop_nulls()
    df = df.with_columns([
        pl.col("Trading time")
        .str.strptime(pl.Datetime, format="%H:%M:%S.%3f")
        .alias("Trading time")
    ])
    df = df.with_columns([
        pl.col("Trading time").dt.truncate("5m").cast(pl.Time).alias("window_start")
    ])

    result = df.group_by(["ID", "window_start"]).agg([
        pl.col("Last").first().alias("first_price"),
        pl.col("Last").last().alias("last_price"),
        pl.col("Last").max().alias("max_price"),
        pl.col("Last").min().alias("min_price"),
        pl.col("Last").len().alias("movements"),
        pl.col("Trading time").last().cast(pl.Time).alias("last_update")
    ]).sort(["ID", "window_start"])

    # Print all rows
    pl.Config.set_tbl_rows(-1)
    print(result)
    
    return result


if __name__ == "__main__":
    app()
