start-core:
    docker compose -f docker/docker-compose.yml rm --stop --volumes --force
    docker compose  -f docker/docker-compose.yml -f docker/compose.jetstream.yml up

start-jetsream:
    docker compose -f docker/docker-compose.yml rm --stop --volumes --force
    docker compose -f docker/docker-compose.yml -f docker/compose.nats_core.yml up

start-consumer:
    cd consumer && cargo run -- core by-exchange

start-ingester-all:
    cd ingester && uv run main.py ingest ../data/debs2022-gc-trading-day-08-11-21.csv ../data/debs2022-gc-trading-day-09-11-21.csv ../data/debs2022-gc-trading-day-10-11-21.csv ../data/debs2022-gc-trading-day-11-11-21.csv ../data/debs2022-gc-trading-day-12-11-21.csv ../data/debs2022-gc-trading-day-13-11-21.csv ../data/debs2022-gc-trading-day-14-11-21.csv --flush-interval 1000 --entity ALE15.FR
