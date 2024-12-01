start-core:
    docker compose -f docker/docker-compose.yml rm --stop --volumes --force
    docker compose  -f docker/docker-compose.yml -f docker/compose.jetstream.yml up

start-jetsream:
    docker compose -f docker/docker-compose.yml rm --stop --volumes --force
    docker compose -f docker/docker-compose.yml -f docker/compose.nats_core.yml up

consume mode partition:
    cd consumer && cargo run --release -- {{mode}} {{partition}}

ingest-global entity="":
    #!/usr/bin/env sh
    cd ingester
    if command -v uv >/dev/null 2>&1; then
        RUNNER="uv run"
    else
        RUNNER="python3"
    fi
    echo "Using runner '${RUNNER}'"
    echo "Ingesting all files"
    if [ -z "{{entity}}" ]; then
        $RUNNER main.py ingest \
            ../data/debs2022-gc-trading-day-08-11-21.csv \
            ../data/debs2022-gc-trading-day-09-11-21.csv \
            ../data/debs2022-gc-trading-day-10-11-21.csv \
            ../data/debs2022-gc-trading-day-11-11-21.csv \
            ../data/debs2022-gc-trading-day-12-11-21.csv \
            ../data/debs2022-gc-trading-day-13-11-21.csv \
            ../data/debs2022-gc-trading-day-14-11-21.csv \
            --partition=global
    else
        $RUNNER main.py ingest \
            ../data/debs2022-gc-trading-day-08-11-21.csv \
            ../data/debs2022-gc-trading-day-09-11-21.csv \
            ../data/debs2022-gc-trading-day-10-11-21.csv \
            ../data/debs2022-gc-trading-day-11-11-21.csv \
            ../data/debs2022-gc-trading-day-12-11-21.csv \
            ../data/debs2022-gc-trading-day-13-11-21.csv \
            ../data/debs2022-gc-trading-day-14-11-21.csv \
            --entity {{entity}} --partition=global
    fi

ingest-by-exchange:
    #!/usr/bin/env sh
    cd ingester
    if command -v uv >/dev/null 2>&1; then
        RUNNER="uv run"
    else
        RUNNER="python3"
    fi
    echo "Using runner '${RUNNER}'"
    echo "Ingesting all files, partitioning by exchange"
    $RUNNER main.py ingest \
        ../data/debs2022-gc-trading-day-08-11-21.csv \
        ../data/debs2022-gc-trading-day-09-11-21.csv \
        ../data/debs2022-gc-trading-day-10-11-21.csv \
        ../data/debs2022-gc-trading-day-11-11-21.csv \
        ../data/debs2022-gc-trading-day-12-11-21.csv \
        ../data/debs2022-gc-trading-day-13-11-21.csv \
        ../data/debs2022-gc-trading-day-14-11-21.csv \
        --partition=exchange

ingest-by-id:
    #!/usr/bin/env sh
    cd ingester
    if command -v uv >/dev/null 2>&1; then
        RUNNER="uv run"
    else
        RUNNER="python3"
    fi
    echo "Using runner '${RUNNER}'"
    echo "Ingesting all files, sequential mode"
    $RUNNER main.py ingest \
        ../data/debs2022-gc-trading-day-08-11-21.csv \
        ../data/debs2022-gc-trading-day-09-11-21.csv \
        ../data/debs2022-gc-trading-day-10-11-21.csv \
        ../data/debs2022-gc-trading-day-11-11-21.csv \
        ../data/debs2022-gc-trading-day-12-11-21.csv \
        ../data/debs2022-gc-trading-day-13-11-21.csv \
        ../data/debs2022-gc-trading-day-14-11-21.csv --ingestion-mode=sequential

ingest-parallel:
    #!/usr/bin/env sh
    cd ingester
    if command -v uv >/dev/null 2>&1; then
        RUNNER="uv run"
    else
        RUNNER="python3"
    fi
    echo "Using runner '${RUNNER}'"
    echo "Ingesting all files, parallel mode"
    $RUNNER main.py ingest \
        ../data/debs2022-gc-trading-day-08-11-21.csv \
        ../data/debs2022-gc-trading-day-09-11-21.csv \
        ../data/debs2022-gc-trading-day-10-11-21.csv \
        ../data/debs2022-gc-trading-day-11-11-21.csv \
        ../data/debs2022-gc-trading-day-12-11-21.csv \
        ../data/debs2022-gc-trading-day-13-11-21.csv \
        ../data/debs2022-gc-trading-day-14-11-21.csv --ingestion-mode=parallel