import polars as pl
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator, FuncFormatter


def plot_cpu_usage(input: str, output: str):
    df = pl.read_csv(input)
    df = df.with_columns((pl.col("timestamp") * 1000).cast(pl.Datetime(time_unit="ms")))
    df = df.drop("memory")

    # Align timestamps to start from 0
    start_time = df["timestamp"][0]
    df = df.with_columns(((pl.col("timestamp") - start_time) / 1000).cast(pl.Int64))
    df = df.filter(pl.col("timestamp") >= 0)  # Remove any negative values

    plt.figure(figsize=(10, 6))
    plt.plot(df["timestamp"], df["cpu_percent"], "b-")
    plt.ylabel("CPU %")
    plt.title("CPU Usage Over Time")
    plt.ylim(0, 100)  # Ensure CPU percentage is between 0 and 100

    def x_fmt(x, _):
        return f"{int(x)}s"

    def y_fmt(y, _):
        return f"{int(y)}%"

    # Set major locator for x-axis to space out ticks
    plt.gca().xaxis.set_major_locator(MultipleLocator(60))  # Every 60 seconds

    # Rotate the x-axis labels for better readability
    plt.xticks(rotation=45)

    # Format x and y axis labels
    plt.gca().xaxis.set_major_formatter(FuncFormatter(x_fmt))
    plt.gca().yaxis.set_major_formatter(FuncFormatter(y_fmt))

    # Add grid for better readability
    plt.grid(True)

    # Ensure x-axis starts from 0
    plt.xlim(left=0)

    plt.savefig(output, dpi=300, bbox_inches="tight")
    plt.close()


if __name__ == "__main__":
    plot_cpu_usage("nat-multi-7-profile.csv", "nats-core-multi-7-cpu.pdf")
    plot_cpu_usage("jetstream-multi-7-profile.csv", "jetstream-multi-7-cpu.pdf")
