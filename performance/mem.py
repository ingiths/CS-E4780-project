import polars as pl
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator, FuncFormatter

def clean_memory_data(memory_str: str) -> int:
    memory_str = memory_str.rstrip('+-')

    if memory_str.endswith('K'):
        return int(memory_str[:-1])
    elif memory_str.endswith('M'):
        return int(memory_str[:-1]) * 1_000
    else:
        return int(memory_str)

def plot_mem_usage(input: str, output: str):
    df = pl.read_csv(input)
    df = df.with_columns((pl.col('timestamp') * 1000).cast(pl.Datetime(time_unit="ms")))
    df = df.with_columns(pl.col('memory').map_elements(clean_memory_data, return_dtype=pl.Int64))
    df = df.drop('cpu_percent')

    start_time = df['timestamp'][0]
    df = df.with_columns((pl.col('timestamp') - start_time) / 1000).cast(pl.Int64) # Stupid conversion to seconds

    plt.figure(figsize=(10,6))
    plt.plot(df['timestamp'], df['memory'], 'b-')
    plt.ylabel('Memory usage (KB)')
    plt.title('Memory Usage Over Time')

    def x_fmt(x, _): return f'{int(x)}s'
    def y_fmt(y, _): return f'{int(y)} KB'

    plt.gca().xaxis.set_major_formatter(FuncFormatter(x_fmt))
    plt.gca().yaxis.set_major_formatter(FuncFormatter(y_fmt))
    plt.gca().xaxis.set_major_locator(MultipleLocator(30))


    plt.grid(True)

    plt.savefig(output, dpi=300, bbox_inches='tight')
    plt.close()

if __name__ == "__main__":
    plot_mem_usage("nats-core-single.csv", "nats-core-single-mem.pdf")
    plot_mem_usage("jetstream-single.csv", "jetstream-single-mem.pdf")
    plot_mem_usage("nat-single-profile.csv", "tmp.pdf")