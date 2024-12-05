alias s := start
alias c := consume

# Starts either a Core NATS server or a JetStream server
start mode:
    #!/usr/bin/env sh
    if [ "{{mode}}" = "core" ]; then
        docker compose -f docker/docker-compose.yml -f docker/compose.nats_core.yml rm --stop --volumes --force
        docker compose  -f docker/docker-compose.yml -f docker/compose.nats_core.yml up --remove-orphans
    elif [ "{{mode}}" = "jetstream" ]; then
        docker compose -f docker/docker-compose.yml -f docker/compose.jetstream.yml rm --stop --volumes --force
        docker compose  -f docker/docker-compose.yml -f docker/compose.jetstream.yml up --remove-orphans
    else
        echo "Error: must be 'core' or 'jetstream'. '{{mode}}'' is invalid!" >&2
    fi


# Start a consumer that listens for messages related to tick data
consume mode partition batch-size="500" flush-period="500" count="1" :
    #!/usr/bin/env sh
    bin="trash"
    if [ "{{mode}}" = "core" ]; then
        bin=nats-core-consumer
    elif [ "{{mode}}" = "jetstream" ]; then
        bin=jetstream-consumer
    else
        echo "Error: mode must be 'core' or 'jetstream'" >&2
        exit 1
    fi
    if [ {{partition}} = "multi" ]; then
        cd consumer && cargo run --release --bin ${bin} -- -b {{batch-size}} -f {{flush-period}} {{partition}} -n {{count}}
    else 
        cd consumer && cargo run --release --bin ${bin} -- -b {{batch-size}} -f {{flush-period}} {{partition}}
    fi


# Start some number of ingesters that send messages to a single subject named `exchange`
ingest-single mode consumer-count entity="":
    #!/usr/bin/env sh
    if [ "{{mode}}" = "core" ]; then
        python_mode="nats_core"
    elif [ "{{mode}}" = "jetstream" ]; then
        python_mode="jetstream"
    else
        echo "Error: mode must be 'core' or 'jetstream'" >&2
        exit 1
    fi
    cd ingester/ingester
    if command -v uv >/dev/null 2>&1; then
        RUNNER="uv run"
    else
        RUNNER="python3"
    fi
    echo "Using runner '${RUNNER}'"
    echo "Ingesting all files"
    if [ -z "{{entity}}" ]; then
        $RUNNER main.py ${python_mode} single \
            ../../data/debs2022-gc-trading-day-08-11-21.csv \
            ../../data/debs2022-gc-trading-day-09-11-21.csv \
            ../../data/debs2022-gc-trading-day-10-11-21.csv \
            ../../data/debs2022-gc-trading-day-11-11-21.csv \
            ../../data/debs2022-gc-trading-day-12-11-21.csv \
            ../../data/debs2022-gc-trading-day-13-11-21.csv \
            ../../data/debs2022-gc-trading-day-14-11-21.csv \
            --consumer-count={{consumer-count}}
    else
        $RUNNER main.py ${python_mode} single \
            ../../data/debs2022-gc-trading-day-08-11-21.csv \
            ../../data/debs2022-gc-trading-day-09-11-21.csv \
            ../../data/debs2022-gc-trading-day-10-11-21.csv \
            ../../data/debs2022-gc-trading-day-11-11-21.csv \
            ../../data/debs2022-gc-trading-day-12-11-21.csv \
            ../../data/debs2022-gc-trading-day-13-11-21.csv \
            ../../data/debs2022-gc-trading-day-14-11-21.csv \
            --entity {{entity}} --consumer-count={{consumer-count}}
    fi


# Start three ingesters/producer that partition the data and send it to the streams [exchange.ETR, exchange.FR, exchange.NL]
ingest-by-exchange mode:
    #!/usr/bin/env sh
    if [ "{{mode}}" = "core" ]; then
        python_mode="nats_core"
    elif [ "{{mode}}" = "jetstream" ]; then
        python_mode="jetstream"
    else
        echo "Error: mode must be 'core' or 'jetstream'" >&2
        exit 1
    fi
    cd ingester/ingester
    if command -v uv >/dev/null 2>&1; then
        RUNNER="uv run"
    else
        RUNNER="python3"
    fi
    echo "Using runner '${RUNNER}'"
    echo "Ingesting all files, partitioning by exchange"
    $RUNNER main.py ${python_mode} exchange \
        ../../data/debs2022-gc-trading-day-08-11-21.csv \
        ../../data/debs2022-gc-trading-day-09-11-21.csv \
        ../../data/debs2022-gc-trading-day-10-11-21.csv \
        ../../data/debs2022-gc-trading-day-11-11-21.csv \
        ../../data/debs2022-gc-trading-day-12-11-21.csv \
        ../../data/debs2022-gc-trading-day-13-11-21.csv \
        ../../data/debs2022-gc-trading-day-14-11-21.csv


# Start N ingesters/producer that partition the data and send it to the streams exchange.0 ... exchange.N - 1
ingest-multi mode count="1":
    #!/usr/bin/env sh
    if [ "{{mode}}" = "core" ]; then
        python_mode="nats_core"
    elif [ "{{mode}}" = "jetstream" ]; then
        python_mode="jetstream"
    else
        echo "Error: mode must be 'core' or 'jetstream'" >&2
        exit 1
    fi
    cd ingester/ingester
    if command -v uv >/dev/null 2>&1; then
        RUNNER="uv run"
    else
        RUNNER="python3"
    fi
    echo "Using runner '${RUNNER}'"
    echo "Ingesting all files, creating {{count}} ingesters"
    $RUNNER main.py ${python_mode} multi \
        ../../data/debs2022-gc-trading-day-08-11-21.csv \
        ../../data/debs2022-gc-trading-day-09-11-21.csv \
        ../../data/debs2022-gc-trading-day-10-11-21.csv \
        ../../data/debs2022-gc-trading-day-11-11-21.csv \
        ../../data/debs2022-gc-trading-day-12-11-21.csv \
        ../../data/debs2022-gc-trading-day-13-11-21.csv \
        ../../data/debs2022-gc-trading-day-14-11-21.csv \
        --consumer-count={{count}}

# Profile CPU and memory usage of a program
profile program file-name rate="1":
    #!/usr/bin/env bash
    output_file=performance/{{file-name}}.csv
    pid=$(pgrep {{program}})
    echo "timestamp,cpu_percent,memory" > $output_file
    while true; do
        timestamp=$(date +%s)
        metrics=$(/usr/bin/top -pid $pid -l 2 -stats cpu,mem | tail -n 1)
        cpu=$(echo $metrics | awk '{print $1'})
        mem=$(echo $metrics | awk '{print $2'})
        echo "$timestamp,$cpu,$mem" >> $output_file
        sleep {{rate}} 
    done
