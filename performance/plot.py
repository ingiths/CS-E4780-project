import matplotlib.pyplot as plt
import numpy as np
import json

source = {}
measurement = "influx"  # window, influx, total

# Open and read the JSON file
with open("single.json", "r") as file:
    data = json.load(file)
    source["single"] = data[measurement]

# Open and read the JSON file
with open("byExchange.json", "r") as file:
    data = json.load(file)
    source["byExchange"] = data[measurement]

# Open and read the JSON file
with open("multi-3.json", "r") as file:
    data = json.load(file)
    source["multi-3"] = data[measurement]

# Open and read the JSON file
with open("multi-5.json", "r") as file:
    data = json.load(file)
    source["multi-5"] = data[measurement]

# Open and read the JSON file
with open("multi-10.json", "r") as file:
    data = json.load(file)
    source["multi-10"] = data[measurement]

# Prepare data for plotting
categories = list(data.keys())
metrics = ["min", "max", "avg"]
single = [source["single"][metric] for metric in metrics]
byExchange = [source["byExchange"][metric] for metric in metrics]
multi3 = [source["multi-3"][metric] for metric in metrics]
multi5 = [source["multi-5"][metric] for metric in metrics]
multi10 = [source["multi-10"][metric] for metric in metrics]

# Set the positions of the bars on the x-axis
x = np.arange(len(metrics))

# Bar width
width = 0.1

# Create the plot
fig, ax = plt.subplots(figsize=(5, 6))

# Plotting the bars for each category
ax.bar(x - 2 * width, single, width, label="Single", color="pink")
ax.bar(x - width, byExchange, width, label="By Exchange (3)", color="skyblue")
ax.bar(x, multi3, width, label="3 consumers", color="khaki")
ax.bar(x + width, multi5, width, label="5 consumers", color="lightgreen")
ax.bar(x + 2 * width, multi10, width, label="10 consumers", color="paleturquoise")

# Adding labels and title
ax.set_xlabel("")
ax.set_ylabel("Time (milliseconds)")
ax.set_title("Min, Avg, and Max Latency Comparison")
ax.set_xticks(x)
ax.set_xticklabels(metrics)
ax.legend()

# Show the plot
plt.tight_layout()
plt.show()
