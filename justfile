start-core:
    docker compose -f docker/docker-compose.yml -f docker/compose.nats_core.yml rm --stop --volumes --force
    docker compose  -f docker/docker-compose.yml -f docker/compose.nats_core.yml up --remove-orphans


consume partition batch-size="500" flush-period="500" count="1" :
    #!/usr/bin/env sh
    if [ {{partition}} = "multi" ]; then
        cd consumer && cargo run --release -- -b {{batch-size}} -f {{flush-period}} {{partition}} -n {{count}}
    else
        cd consumer && cargo run --release -- -b {{batch-size}} -f {{flush-period}} {{partition}}
    fi


numerical entity date:
    #!/usr/bin/env sh
    cd ingester
    if command -v uv >/dev/null 2>&1; then
        RUNNER="uv run"
    else
        RUNNER="python3"
    fi
    $RUNNER analyze.py numerical {{entity}} {{date}}


analytical analytical entity:
    #!/usr/bin/env sh
    cd ingester
    if command -v uv >/dev/null 2>&1; then
        RUNNER="uv run"
    else
        RUNNER="python3"
    fi
    $RUNNER analyze.py analytical  ../{{analytical}} {{entity}}


compare data_file entity:
    #!/usr/bin/env sh
    cd ingester
    if command -v uv >/dev/null 2>&1; then
        RUNNER="uv run"
    else
        RUNNER="python3"
    fi
    $RUNNER analyze.py compare  ../{{data_file}} {{entity}}


ingest-single consumer-count entity="":
    #!/usr/bin/env sh
    cd ingester/ingester
    if command -v uv >/dev/null 2>&1; then
        RUNNER="uv run"
    else
        RUNNER="python3"
    fi
    echo "Using runner '${RUNNER}'"
    echo "Ingesting all files"
    if [ -z "{{entity}}" ]; then
        $RUNNER main.py \
            ../../data/debs2022-gc-trading-day-08-11-21.csv \
            ../../data/debs2022-gc-trading-day-09-11-21.csv \
            ../../data/debs2022-gc-trading-day-10-11-21.csv \
            ../../data/debs2022-gc-trading-day-11-11-21.csv \
            ../../data/debs2022-gc-trading-day-12-11-21.csv \
            ../../data/debs2022-gc-trading-day-13-11-21.csv \
            ../../data/debs2022-gc-trading-day-14-11-21.csv \
            --partition=single --consumer-count={{consumer-count}}
    else
        $RUNNER main.py \
            ../../data/debs2022-gc-trading-day-08-11-21.csv \
            ../../data/debs2022-gc-trading-day-09-11-21.csv \
            ../../data/debs2022-gc-trading-day-10-11-21.csv \
            ../../data/debs2022-gc-trading-day-11-11-21.csv \
            ../../data/debs2022-gc-trading-day-12-11-21.csv \
            ../../data/debs2022-gc-trading-day-13-11-21.csv \
            ../../data/debs2022-gc-trading-day-14-11-21.csv \
            --entity {{entity}} --partition=global --consumer-count={{consumer-count}}
    fi


ingest-by-exchange:
    #!/usr/bin/env sh
    cd ingester/ingester
    if command -v uv >/dev/null 2>&1; then
        RUNNER="uv run"
    else
        RUNNER="python3"
    fi
    echo "Using runner '${RUNNER}'"
    echo "Ingesting all files, partitioning by exchange"
    $RUNNER main.py \
        ../../data/debs2022-gc-trading-day-08-11-21.csv \
        ../../data/debs2022-gc-trading-day-09-11-21.csv \
        ../../data/debs2022-gc-trading-day-10-11-21.csv \
        ../../data/debs2022-gc-trading-day-11-11-21.csv \
        ../../data/debs2022-gc-trading-day-12-11-21.csv \
        ../../data/debs2022-gc-trading-day-13-11-21.csv \
        ../../data/debs2022-gc-trading-day-14-11-21.csv \
        --partition=exchange


ingest-multi count="1":
    #!/usr/bin/env sh
    cd ingester/ingester
    if command -v uv >/dev/null 2>&1; then
        RUNNER="uv run"
    else
        RUNNER="python3"
    fi
    echo "Using runner '${RUNNER}'"
    echo "Ingesting all files, creating {{count}} ingesters"
    $RUNNER main.py \
        ../../data/debs2022-gc-trading-day-08-11-21.csv \
        ../../data/debs2022-gc-trading-day-09-11-21.csv \
        ../../data/debs2022-gc-trading-day-10-11-21.csv \
        ../../data/debs2022-gc-trading-day-11-11-21.csv \
        ../../data/debs2022-gc-trading-day-12-11-21.csv \
        ../../data/debs2022-gc-trading-day-13-11-21.csv \
        ../../data/debs2022-gc-trading-day-14-11-21.csv \
        --partition=multi --consumer-count={{count}}