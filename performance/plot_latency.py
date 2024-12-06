import matplotlib.pyplot as plt
import numpy as np
import json


# Load data from JSON files
with open("jetstream-single-perf.json", "r") as f:
    jetstream_single = json.load(f)

# Load data from JSON files
with open("jetstream-by-exchange-perf.json", "r") as f:
    jetstream_byexchange = json.load(f)

# Load data from JSON files
with open("jetstream-multi-5-perf.json", "r") as f:
    jetstream_multi_5 = json.load(f)

# Load data from JSON files
with open("jetstream-multi-7-perf.json", "r") as f:
    jetstream_multi_7 = json.load(f)

# Load data from JSON files
with open("nat-single-perf.json", "r") as f:
    nat_single = json.load(f)

# Load data from JSON files
with open("nat-by-exchange-perf.json", "r") as f:
    nat_byexchange = json.load(f)

# Load data from JSON files
with open("nat-multi-5-perf.json", "r") as f:
    nat_multi_5 = json.load(f)

# Load data from JSON files
with open("nat-multi-7-perf.json", "r") as f:
    nat_multi_7 = json.load(f)

action = "influx"
mode = "min"


# Define the data
groups = ["Jetstream", "NATS"]  # Two groups on x-axis
consumer_names = ["1", "3", "5", "7"]  # Consumer names
jetstream_latencies = [
    jetstream_single[action][mode],
    jetstream_byexchange[action][mode],
    jetstream_multi_5[action][mode],
    jetstream_multi_7[action][mode],
]  # Latencies for Jetstream
nats_latencies = [
    nat_single[action][mode],
    nat_byexchange[action][mode],
    nat_multi_5[action][mode],
    nat_multi_7[action][mode],
]  # Latencies for NATS

# Combine data into a list of lists
data = [jetstream_latencies, nats_latencies]

# Set up bar positions
x = np.arange(len(groups))  # Label locations for groups (Jetstream, NATS)
width = 0.2  # Width of each bar
offsets = np.linspace(
    -1.5 * width, 1.5 * width, len(consumer_names)
)  # Offsets for each consumer's bars

# Create the plot
fig, ax = plt.subplots(figsize=(10, 6))

# Plot bars for each consumer within each group
colors = ["skyblue", "orange", "green", "purple"]  # Different colors for consumers
for i, consumer in enumerate(consumer_names):
    ax.bar(
        x + offsets[i],
        [data[0][i], data[1][i]],
        width,
        label=f"{consumer} consumers ",
        color=colors[i],
    )

# Add labels, title, and legend
ax.set_xlabel("Groups")
ax.set_ylabel(f"{mode} latency (ms)")
ax.set_title(f"{mode} Latency by Group and Consumers")
ax.set_xticks(x)
ax.set_xticklabels(groups)
ax.legend(title="Consumers", loc="upper left")

# Add data labels
for i, consumer in enumerate(consumer_names):
    for j, latency in enumerate([data[0][i], data[1][i]]):
        ax.text(
            j + offsets[i], latency + 0.1, f"{latency:.1f}", ha="center", va="bottom"
        )

# Show the plot
plt.tight_layout()
plt.show()
