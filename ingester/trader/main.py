import asyncio

import typer
import nats

app = typer.Typer(pretty_exceptions_enable=False)

NATS_SERVER = "nats://localhost:4223"


@app.command()
def watch(ids: list[str]):
    async def start_watching(ids: list[str]):
        nc = await nats.connect(NATS_SERVER)
        js = nc.jetstream()
        jsm = nc.jsm()

        print("Listening to breakout-events from the following IDs")
        for id in ids:
            print(f"- {id}")

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

    asyncio.run(start_watching(ids))


if __name__ == "__main__":
    app()
