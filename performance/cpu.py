import polars as pl
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator, FuncFormatter

def plot_cpu_usage(input: str, output: str):
    df = pl.read_csv(input)
    df = df.with_columns((pl.col('timestamp') * 1000).cast(pl.Datetime(time_unit="ms")))
    df = df.drop('memory')

    start_time = df['timestamp'][0]
    df = df.with_columns((pl.col('timestamp') - start_time) / 1000).cast(pl.Int64) # Stupid conversion to seconds

    plt.figure(figsize=(10,6))
    plt.plot(df['timestamp'], df['cpu_percent'], 'b-')
    plt.ylabel('CPU %')
    plt.title('CPU Usage Over Time')
    plt.ylim(0, 100)

    def x_fmt(x, _): return f'{int(x)}s'
    def y_fmt(y, _): return f'{int(y)}%'

    plt.gca().xaxis.set_major_formatter(FuncFormatter(x_fmt))
    plt.gca().yaxis.set_major_formatter(FuncFormatter(y_fmt))
    plt.gca().xaxis.set_major_locator(MultipleLocator(30))

    plt.grid(True)

    plt.savefig(output, dpi=300, bbox_inches='tight')
    plt.close()

if __name__ == "__main__":
    plot_cpu_usage("nats-core-single.csv", "nats-core-single-mem.pdf")
    plot_cpu_usage("jetstream-single.csv", "jetstream-single-mem.pdf")