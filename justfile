start-core:
    docker compose -f docker/docker-compose.yml rm --stop --volumes --force
    docker compose  -f docker/docker-compose.yml -f docker/compose.jetstream.yml up

start-jetsream:
    docker compose -f docker/docker-compose.yml rm --stop --volumes --force
    docker compose -f docker/docker-compose.yml -f docker/compose.nats_core.yml up

consume mode partition:
    cd consumer && cargo run --release -- {{mode}} {{partition}}

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
            --flush-interval 100 --entity {{entity}}
    else
        echo "Ingesting single file ../data/debs2022-gc-trading-day-{{date}}.csv"
        $RUNNER main.py ingest ../data/debs2022-gc-trading-day-{{date}}.csv --flush-interval 1000 --entity {{entity}}
    fi

ingest-all:
    #!/usr/bin/env sh
    cd ingester
    if command -v uv >/dev/null 2>&1; then
        RUNNER="uv run"
    else
        RUNNER="python3"
    fi

    echo "Using runner '${RUNNER}'"
    
    echo "Ingesting all files"
    $RUNNER main.py ingest \
        ../data/debs2022-gc-trading-day-08-11-21.csv \
        ../data/debs2022-gc-trading-day-09-11-21.csv \
        ../data/debs2022-gc-trading-day-10-11-21.csv \
        ../data/debs2022-gc-trading-day-11-11-21.csv \
        ../data/debs2022-gc-trading-day-12-11-21.csv \
        ../data/debs2022-gc-trading-day-13-11-21.csv \
        ../data/debs2022-gc-trading-day-14-11-21.csv \
        --flush-interval 100
