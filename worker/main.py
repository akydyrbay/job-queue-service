"""Worker process: claim a job, simulate work, record the outcome, repeat.

Run with `python -m worker.main`. Start as many as you like; they coordinate
only through the database.
"""
import logging
import os
import random
import secrets
import signal
import socket
import sys
import time

import psycopg

from app.config import Settings, load_settings
from worker import queue

log = logging.getLogger("worker")

GIVE_UP_CHECK_SECONDS = 1.0


def make_worker_id() -> str:
    # The pid makes it easy to match log lines to the process the harness
    # kills; the random suffix keeps ids unique if a pid is reused.
    return f"{socket.gethostname()}-{os.getpid()}-{secrets.token_hex(4)}"


def do_work(job: dict, settings: Settings) -> tuple[str, str | None, str | None]:
    """Pretend to train something. Returns (status, output, error).

    The payload can force behaviour, which tests use:
      {"work_ms": 500}  -> take exactly that long
      {"fail": true}    -> always fail
    Otherwise work takes WORK_MIN_MS..WORK_MAX_MS and fails with FAIL_RATE.
    """
    payload = job["payload"] or {}
    work_ms = payload.get("work_ms", random.randint(settings.work_min_ms, settings.work_max_ms))
    time.sleep(work_ms / 1000)

    if payload.get("fail") or random.random() < settings.fail_rate:
        return "failed", None, "simulated failure"
    return "succeeded", f"done in {work_ms} ms", None


class Worker:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.worker_id = make_worker_id()
        self.stopping = False
        self.conn: psycopg.Connection | None = None
        self.last_give_up_check = 0.0

    def stop(self, *_):
        log.info("event=stopping worker=%s", self.worker_id)
        self.stopping = True

    def run(self):
        log.info(
            "event=started worker=%s lease=%ss max_attempts=%s fail_rate=%s",
            self.worker_id, self.settings.lease_seconds, self.settings.max_attempts,
            self.settings.fail_rate,
        )
        backoff = 0.5
        while not self.stopping:
            try:
                if self.conn is None or self.conn.closed:
                    self.conn = queue.connect(self.settings.database_url)
                    backoff = 0.5
                if not self.step():
                    self.idle()
            except psycopg.OperationalError as exc:
                # Lost the database. Whatever we were holding will be reclaimed
                # when its lease expires, so just reconnect.
                log.warning("event=db_error worker=%s error=%r retry_in=%.1fs",
                            self.worker_id, str(exc).strip(), backoff)
                self.close()
                time.sleep(backoff)
                backoff = min(backoff * 2, 10)
        self.close()
        log.info("event=stopped worker=%s", self.worker_id)

    def step(self) -> bool:
        """Process at most one job. Returns False if there was nothing to do."""
        self.maybe_give_up_exhausted()

        job = queue.claim(
            self.conn, self.worker_id, self.settings.lease_seconds, self.settings.max_attempts
        )
        if job is None:
            return False
        log.info("event=claimed worker=%s job=%s attempt=%s",
                 self.worker_id, job["id"], job["attempts"])

        try:
            status, output, error = do_work(job, self.settings)
        except Exception as exc:
            # A bug in the job (e.g. a bad payload) fails that job, not the worker.
            status, output, error = "failed", None, f"{type(exc).__name__}: {exc}"

        if queue.finish(self.conn, job, self.worker_id, status, output, error):
            log.info("event=%s worker=%s job=%s attempt=%s",
                     status, self.worker_id, job["id"], job["attempts"])
        else:
            log.info("event=stale_rejected worker=%s job=%s attempt=%s",
                     self.worker_id, job["id"], job["attempts"])
        return True

    def maybe_give_up_exhausted(self):
        now = time.monotonic()
        if now - self.last_give_up_check < GIVE_UP_CHECK_SECONDS:
            return
        self.last_give_up_check = now
        for job_id in queue.give_up_exhausted(self.conn, self.settings.max_attempts):
            log.info("event=gave_up worker=%s job=%s", self.worker_id, job_id)

    def idle(self):
        # Jitter so idle workers don't all hit the database at the same moment.
        base = self.settings.poll_interval_ms / 1000
        time.sleep(base + random.uniform(0, base))

    def close(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = None


def main():
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    worker = Worker(load_settings())
    # SIGTERM / Ctrl+C: finish the current job, then exit. SIGKILL can't be
    # handled; recovery from that comes from lease expiry.
    signal.signal(signal.SIGTERM, worker.stop)
    signal.signal(signal.SIGINT, worker.stop)
    worker.run()


if __name__ == "__main__":
    main()
