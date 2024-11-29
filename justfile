start-core:
    docker compose -f docker/docker-compose.yml rm --stop --volumes --force
    docker compose  -f docker/docker-compose.yml -f docker/compose.jetstream.yml up

start-jetsream:
    docker compose -f docker/docker-compose.yml rm --stop --volumes --force
    docker compose -f docker/docker-compose.yml -f docker/compose.nats_core.yml up

start-consumer:
    cd consumer && cargo run -- core by-exchange

start-ingester-all:
    cd ingester && uv run main.py ingest ../data/debs2022-gc-trading-day-08-11-21.csv ../data/debs2022-gc-trading-day-09-11-21.csv ../data/debs2022-gc-trading-day-10-11-21.csv ../data/debs2022-gc-trading-day-11-11-21.csv ../data/debs2022-gc-trading-day-12-11-21.csv ../data/debs2022-gc-trading-day-13-11-21.csv ../data/debs2022-gc-trading-day-14-11-21.csv --flush-interval 1000 --entity 645290.ETR


# If no date is provided, process all dates from 08-11-21 to 14-11-21
ingest entity *date="":
    #!/usr/bin/env sh
    cd ingester
    if command -v uv >/dev/null 2>&1; then
        RUNNER="uv run"
    else
        RUNNER="python3"
    fi

    echo "Using runner '${RUNNER}'"
    
    if [ -z "{{date}}" ]; then
        echo "Ingesting all files"
        $RUNNER main.py ingest \
            ../data/debs2022-gc-trading-day-08-11-21.csv \
            ../data/debs2022-gc-trading-day-09-11-21.csv \
            ../data/debs2022-gc-trading-day-10-11-21.csv \
            ../data/debs2022-gc-trading-day-11-11-21.csv \
            ../data/debs2022-gc-trading-day-12-11-21.csv \
            ../data/debs2022-gc-trading-day-13-11-21.csv \
            ../data/debs2022-gc-trading-day-14-11-21.csv \
            --flush-interval 1000 --entity {{entity}}
    else
        echo "Ingesting single file ../data/debs2022-gc-trading-day-{{date}}.csv"
        $RUNNER main.py ingest ../data/debs2022-gc-trading-day-{{date}}.csv --flush-interval 1000 --entity {{entity}}
    fi