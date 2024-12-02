from influxdb_client import InfluxDBClient
import json
import os
from collections import defaultdict

# Setup InfluxDB client
url = "http://localhost:8086"
token = "token"
org = "trading-org"
bucket = "trading_bucket"
client = InfluxDBClient(url=url, token=token)
query_api = client.query_api()

# Define the query to execute
query = """
from(bucket: "trading_bucket")
  |> range(start: -15h)
  |> filter(fn: (r) => r["_measurement"] == "perf")
  |> filter(fn: (r) => r["_field"] == "influx_write_end" or r["_field"] == "window_creation_end" or r["_field"] == "window_creation_start")
"""


def query_influxdb():
    try:
        # Store the results
        perf = []

        # Query the data
        result = query_api.query(query, org=org)

        # Process the rows returned by InfluxDB
        for table in result:
            for record in table.records:
                perf.append(record.values)

        # Group the data by 'id' and 'window_number'
        group_by_id = defaultdict(list)
        for row in perf:
            group_by_id[row["id"]].append(row)

        window_creation_time_array = []
        influx_write_time_array = []
        total_latency_array = []

        # Group and calculate the necessary values
        for id, rows in group_by_id.items():
            group_by_win_num = defaultdict(list)
            for row in rows:
                group_by_win_num[row["window_number"]].append(row)

            for wnum, window_rows in group_by_win_num.items():
                influx_write_end = next(
                    r["_value"]
                    for r in window_rows
                    if r["_field"] == "influx_write_end"
                )
                window_creation_end = next(
                    r["_value"]
                    for r in window_rows
                    if r["_field"] == "window_creation_end"
                )
                window_creation_start = next(
                    r["_value"]
                    for r in window_rows
                    if r["_field"] == "window_creation_start"
                )

                window_creation_time = window_creation_end - window_creation_start
                influx_write_time = influx_write_end - window_creation_end
                total_latency = window_creation_time + influx_write_time

                window_creation_time_array.append(window_creation_time)
                influx_write_time_array.append(influx_write_time)
                total_latency_array.append(total_latency)

        # Calculate averages, min, and max
        average_window_creation_time = sum(window_creation_time_array) / len(
            window_creation_time_array
        )
        average_influx_write_time = sum(influx_write_time_array) / len(
            influx_write_time_array
        )
        average_total_latency = sum(total_latency_array) / len(total_latency_array)

        max_window_creation_time = max(window_creation_time_array)
        max_influx_write_time = max(influx_write_time_array)
        max_total_latency = max(total_latency_array)

        min_window_creation_time = min(window_creation_time_array)
        min_influx_write_time = min(influx_write_time_array)
        min_total_latency = min(total_latency_array)

        # Print results
        print(f"Average Window Creation Time: {average_window_creation_time:.3f}")
        print(f"Average Influx Write Time: {average_influx_write_time:.3f}")
        print(f"Average Total Latency: {average_total_latency:.3f}")
        print(f"Max Window Creation Time: {max_window_creation_time}")
        print(f"Max Influx Write Time: {max_influx_write_time}")
        print(f"Max Total Latency: {max_total_latency}")
        print(f"Min Window Creation Time: {min_window_creation_time}")
        print(f"Min Influx Write Time: {min_influx_write_time}")
        print(f"Min Total Latency: {min_total_latency}")

        return {
            "influx": {
                "avg": average_influx_write_time,
                "min": min_influx_write_time,
                "max": max_influx_write_time,
            },
            "window": {
                "avg": average_window_creation_time,
                "min": min_window_creation_time,
                "max": max_window_creation_time,
            },
            "total": {
                "avg": average_window_creation_time + average_influx_write_time,
                "min": min_window_creation_time + min_influx_write_time,
                "max": max_window_creation_time + max_influx_write_time,
            },
        }
    except Exception as e:
        print(f"Error executing query: {e}")
        return None


def get_data(filename):
    try:
        data = query_influxdb()
        if data:
            # Write the data to a JSON file
            with open(f"{filename}.json", "w") as f:
                json.dump(data, f, indent=2)
            print(f"File written successfully as {filename}.json")
        else:
            print("No data returned from InfluxDB.")
    except Exception as e:
        print(f"Error in get_data: {e}")


# Call the function to get data and write it to a file
get_data("multi-10")
