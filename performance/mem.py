import polars as pl
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator, FuncFormatter


def clean_memory_data(memory_str: str) -> int:
    memory_str = memory_str.rstrip("+-")

    if memory_str.endswith("K"):
        return int(memory_str[:-1])
    elif memory_str.endswith("M"):
        return int(memory_str[:-1]) * 1_000
    else:
        return int(memory_str)


def plot_mem_usage(input: str, output: str):
    df = pl.read_csv(input)
    df = df.with_columns((pl.col("timestamp") * 1000).cast(pl.Datetime(time_unit="ms")))
    df = df.with_columns(
        pl.col("memory").map_elements(clean_memory_data, return_dtype=pl.Int64)
    )
    df = df.drop("cpu_percent")

    # Align timestamps to start from 0
    start_time = df["timestamp"][0]
    df = df.with_columns(((pl.col("timestamp") - start_time) / 1000).cast(pl.Int64))
    df = df.filter(pl.col("timestamp") >= 0)  # Remove any negative values

    plt.figure(figsize=(10, 6))
    plt.plot(df["timestamp"], df["memory"], "b-")
    plt.ylabel("Memory usage (KB)")
    plt.title("Memory Usage Over Time")

    def x_fmt(x, _):
        minutes, seconds = divmod(int(x), 60)
        return f"{minutes:02d}:{seconds:02d}"

    def y_fmt(y, _):
        return f"{int(y)} KB"

    plt.gca().xaxis.set_major_formatter(FuncFormatter(x_fmt))
    plt.gca().yaxis.set_major_formatter(FuncFormatter(y_fmt))
    plt.gca().xaxis.set_major_locator(MultipleLocator(60))  # Adjust tick interval
    plt.xticks(rotation=45)

    plt.grid(axis="y", linestyle="--", alpha=0.7)

    # Ensure x-axis starts from 0
    plt.xlim(left=0)

    plt.savefig(output, dpi=300, bbox_inches="tight")
    plt.close()


if __name__ == "__main__":
    plot_mem_usage("nat-multi-7-profile.csv", "nats-core-multi-7-mem.pdf")
    plot_mem_usage("jetstream-multi-7-profile.csv", "jetstream-multi-7-mem.pdf")
