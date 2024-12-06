import asyncio

import typer
import nats
from enum import Enum

app = typer.Typer(pretty_exceptions_enable=False)

NATS_SERVER = "nats://localhost:4222"


class ServerMode(Enum):
    CORE = "core"
    JETSTREAM = "jetstream"


@app.command()
def watch(mode: ServerMode, ids: list[str]):
    async def nats_core_events(ids: list[str]):
        nc = await nats.connect(NATS_SERVER)

        sub = await nc.subscribe("breakouts")

        async for msg in sub.messages:
            if msg.header and msg.header["ID"] in ids:
                print(f"{msg.subject} - {msg.data.decode('utf-8')}")

    async def jetstream_events(ids: list[str]):
        nc = await nats.connect(NATS_SERVER)
        js = nc.jetstream()
        jsm = nc.jsm()

        stream_name = "breakout-events"
        # https://github.com/nats-io/nats.py/commit/eb7da73b2a94f2053b4de43770f051ce222745b5#diff-0c442047748b79f60c3b3eacde0aca291c2c9b2494224adafb5714a9a6447c6bR820
        await jsm.add_stream(name=stream_name, subjects=["breakouts.>"])
        # Create an ephemeral consumer
        # https://docs.nats.io/using-nats/developer/develop_jetstream/consumers#ephemeral-consumers
        await jsm.add_consumer(
            stream_name,
            deliver_policy="last_per_subject",
            ack_policy="explicit",
            filter_subjects=[f"breakouts.{id}" for id in ids],
        )
        sub = await js.pull_subscribe("breakouts.>")

        try:
            while True:
                msg = (await sub.fetch(timeout=None))[0]
                print(f"{msg.subject} - {msg.data.decode('utf-8')}")
                await msg.ack_sync()
        except KeyboardInterrupt:
            print("Tearing down...")
        await sub.unsubscribe()
        await nc.drain()

    print("Listening to breakout messages for the following IDs")
    for id in ids:
        print(f"- {id}")
    if mode == ServerMode.CORE:
        asyncio.run(nats_core_events(ids))
    elif mode == ServerMode.JETSTREAM:
        asyncio.run(jetstream_events(ids))
    else:
        raise ValueError("Invalid server mode")


if __name__ == "__main__":
    app()
