import matplotlib.pyplot as plt
import numpy as np

# Define categories and data
categories = ["Single", "By Exchange", "Multi 5", "Multi 7"]
jetstream_rates = [34042.02, 52417.83, 70473.33, 73556.05]  # Example data for Jetstream
nats_rates = [181803.87, 343105.76, 480165.0, 483562.19]  # Example data for NATS

# Set up bar positions
x = np.arange(len(categories))  # the label locations
width = 0.35  # the width of the bars

# Create the plot
fig, ax = plt.subplots(figsize=(10, 6))
bars1 = ax.bar(x - width / 2, jetstream_rates, width, label="Jetstream")
bars2 = ax.bar(x + width / 2, nats_rates, width, label="NATS")

# Add labels, title, and legend
ax.set_xlabel("Categories")
ax.set_ylabel("Messages/Second")
ax.set_title("Ingestion Rate of Jetstream vs NATS")
ax.set_xticks(x)
ax.set_xticklabels(categories)
ax.legend()

# Add data labels
for bars in [bars1, bars2]:
    ax.bar_label(bars, fmt="%.0f", padding=3)

# Show the plot
plt.tight_layout()
plt.show()
