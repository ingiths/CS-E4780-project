# Trader

This directory contains a single `main.py` which allows users to subscribe to notifications of breakout events that happen in the system.

# Usage

```bash
$ python3 main.py <MODE> <LIST OF IDS>
```

For example, if the system is using Core NATS for message processing

```bash
$ python3 main.py core RDSA.NL ALE15.FR 2ICEU.FR
```
Or if the system is using JetStream

```bash
$ python3 main.py jetstream RDSA.NL ALE15.FR 2ICEU.FR
```