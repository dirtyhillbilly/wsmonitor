#
# Database updater daemon. Listen to kafka events and store
# metrics to database
#

import threading
import queue
from wsmonitor import api
from wsmonitor.config import config

MAX_THREADS = 4

tasks = queue.Queue(maxsize=8)


def worker():
    while True:
        metric = tasks.get()
        print(metric)
        api.database_add_metric(config, *metric)


def dbupdate():
    """
    dbupdate process entry point.
    """

    # Start a bunch of worker threads
    for i in range(MAX_THREADS):
        threading.Thread(target=worker, daemon=False).start()

    # Never-ending loop
    for metric in api.kafka_get_metrics(config):
        # Push a metric to database, blocking if queue is full
        tasks.put(metric)
