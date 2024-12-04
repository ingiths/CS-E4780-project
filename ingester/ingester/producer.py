import polars as pl
import nats
import asyncio
from alive_progress import alive_bar

from utils import create_nats_message

NATS_SERVER = "nats://localhost:4222"


async def jetstream_ingest(
    df: pl.DataFrame,
    exchange: str,
    flush_interval: int = 1000,
    show_progress_bar: bool = False,
):
    # https://stackoverflow.com/questions/70550060/performance-of-nats-jetstream
    nc = await nats.connect(NATS_SERVER)
    js = nc.jetstream()

    acks = []
    if show_progress_bar:
        with alive_bar(len(df)) as bar:
            for i, event in enumerate(df.iter_rows(named=False, buffer_size=1024)):
                acks.append(js.publish(exchange, create_nats_message(*event)))
                if len(acks) > flush_interval:
                    await asyncio.gather(*acks)
                    acks.clear()
                    await nc.flush()
                bar()
    else:
        for i, event in enumerate(df.iter_rows(named=False, buffer_size=1024)):
            acks.append(js.publish(exchange, create_nats_message(*event)))
            if len(acks) > flush_interval:
                await asyncio.gather(*acks)
                acks.clear()
                await nc.flush()

    try:
        await asyncio.gather(*acks)
    finally:
        acks.clear()
    await nc.flush()
    await nc.close()


async def nats_core_ingest(
    df: pl.DataFrame,
    exchange: str,
    flush_interval: int = 1000,
    show_progress_bar: bool = False,
):
    nc = await nats.connect(NATS_SERVER)

    messages = []
    if show_progress_bar:
        with alive_bar(len(df)) as bar:
            for event in df.iter_rows(named=False, buffer_size=1024):
                messages.append(nc.publish(exchange, create_nats_message(*event)))
                if len(messages) > flush_interval:
                    await asyncio.gather(*messages)
                    messages.clear()
                    await nc.flush()
                bar()
    else:
        for event in df.iter_rows(named=False, buffer_size=1024):
            messages.append(nc.publish(exchange, create_nats_message(*event)))
            if len(messages) > flush_interval:
                await asyncio.gather(*messages)
                messages.clear()
                await nc.flush()

    try:
        await asyncio.gather(*messages)
    finally:
        messages.clear()
    await nc.flush()
    await nc.close()
