# Ingester

## Requirements

- [Python 3.12](https://www.python.org/)
- [uv](https://docs.astral.sh/uv/)

## Running

To get a list of CLI flags for the `main.py` use the following command

```bash
uv run main.py --help
# Or
python main.py --help
```

### Ingesting data

```bash
uv run main.py ingest ../data/debs2022-gc-trading-day-08-11-21.csv
# Or
python main.py ingest ../data/debs2022-gc-trading-day-08-11-21.csv
```

### Data exploration

```bash
uv run main.py explore ../data/debs2022-gc-trading-day-08-11-21.csv --limit 100
# Or
python main.py explore ../data/debs2022-gc-trading-day-08-11-21.csv --limit 100
```

## Development - Contributing

1. Clone the repository:
```bash
git clone https://github.com/ingiths/CS-E4780-project
cd CS-E4780-project
```

2. Install development dependencies:
```bash
uv sync
```

Or, use `requirements.txt` as an alternative
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```