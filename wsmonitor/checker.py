#
# Checker daemon. Periodically check website availability and report to kafka queue.
#


import threading
import queue
import urllib.request
import urllib.error
import datetime
import time
import re
from wsmonitor import api
from wsmonitor.config import config

MAX_THREADS = 4
TIMEOUT = 30
CHECK_PERIOD = 20

tasks = queue.Queue(maxsize=8)


def check_url(url_id, url, regexp, timeout):
    now = datetime.datetime.now()
    try:
        conn = urllib.request.urlopen(url, timeout=timeout)
        content = conn.read()
    except urllib.error.HTTPError as e:
        return_code = e.code
    except urllib.error.URLError:
        return_code = 599
    except Exception:
        return_code = -1
    else:
        return_code = 200

    response_time = datetime.datetime.now() - now

    if return_code == 200 and regexp:
        # TODO: handle other charsets from Content-Type
        regexp_check = re.search(regexp, content.decode('utf-8')) is not None
    else:
        regexp_check = None

    api.kafka_send_metric(config, url_id, now,
                          response_time / datetime.timedelta(microseconds=1),
                          return_code, regexp_check)


def loop_over_urls():
    while True:
        start_time = time.time()
        for url in api.url_list(config):
            yield url

        # wait CHECK_PERIOD before looping
        duration = abs(time.time() - start_time)  # abs() prevents time jumps
        if duration < CHECK_PERIOD:
            time.sleep(CHECK_PERIOD - duration)
        else:
            # TODO: should probably  increase queue length
            pass


def worker():
    while True:
        url_id, url, regexp = tasks.get()
        check_url(url_id, url, regexp, TIMEOUT)


def checker():
    """
    Main checker process entry point.
    """

    # Start a bunch of worker threads
    for i in range(MAX_THREADS):
        threading.Thread(target=worker, daemon=False).start()

    # Never-ending loop
    for url_id, url, regexp in loop_over_urls():
        # Push an url to be tested, blocking if queue is full
        tasks.put((url_id, url, regexp))
