# TODO: Ingest with core NATS to benchmark performance

from alive_progress import alive_bar
import threading
import time


def task(bar):
    time.sleep(1)
    bar()


with alive_bar(10) as bar:
    threads = [threading.Thread(target=task, args=(bar,)) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
