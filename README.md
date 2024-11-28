# CS-E4780-project

Scalable Systems and Data Management Course Project Detecting Trading Trends in Financial Tick Data

# Structure

- `docker/` contains a `docker-compose.yml` file to run the [NATS](https://nats.io/) server
- `ingester/` contains the source code for ingesting financial tick data into the NATS server

# How to run

### Containers

```
docker compose  -f docker/docker-compose.yml -f docker/compose.jetstream.yml up
```

### Ingester

- Install dependencies
  ```
  python3 -m venv .venv
  source .venv/bin/activate
  pip3 install -r requirements.txt
  ```
- Run ingester
  ```
  python3 main.py
  python3 main.py --help
  python3 main.py plot-events
  python3 main.py explore ../data/debs2022-gc-trading-day-08-11-21.csv
  python3 main.py ingest ../data/debs2022-gc-trading-day-08-11-21.csv --flush-interval 1000
  ```

### Consumer

- Build
  ```
  cargo build
  ```
- Run
  ```
  cargo run
  ```
