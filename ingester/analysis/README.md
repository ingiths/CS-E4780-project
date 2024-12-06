# Overview

This directory contains various analysis tools for quality checking the system against an analytical approach.

# Usage

## Influx data

```bash
$ python main.py numerical ALE15.FR 2021-11-08
$ python main.py numerical ALE15.FR 2021-11-09
$ python main.py numerical ALE15.FR 2021-11-10
$ python main.py numerical ALE15.FR 2021-11-11
$ python main.py numerical ALE15.FR 2021-11-12
```

## Actual data

```bash
$ python main.py analytical ../../data/debs2022-gc-trading-day-08-11-21.csv RDSA.NL
$ python main.py analytical ../../data/debs2022-gc-trading-day-09-11-21.csv RDSA.NL
$ python main.py analytical ../../data/debs2022-gc-trading-day-10-11-21.csv RDSA.NL
$ python main.py analytical ../../data/debs2022-gc-trading-day-11-11-21.csv RDSA.NL
$ python main.py analytical ../../data/debs2022-gc-trading-day-12-11-21.csv RDSA.NL
$ python main.py analytical ../../data/debs2022-gc-trading-day-13-11-21.csv RDSA.NL
$ python main.py analytical ../../data/debs2022-gc-trading-day-14-11-21.csv RDSA.NL
```

## Comparison

If the table printed is full of 0s, then the systems data matches the analytical data.

```bash
$ python main.py compare ../../data/debs2022-gc-trading-day-08-11-21.csv ALE15.FR
```

This reads data from the file and from the InfluxDB and compares window start times, first, last, min and max prices and how many movements occured in that window.