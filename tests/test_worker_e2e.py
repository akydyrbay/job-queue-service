"""Real worker processes draining a queue, with one killed mid-job.

This is a small version of the chaos harness: 4 workers, 500 jobs, one
kill -9 aimed at a worker that is holding a job.
"""
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from app.db import SCHEMA_PATH
from worker import queue

ROOT = Path(__file__).resolve().parent.parent
N_WORKERS = 4
N_JOBS = 500
LEASE_SECONDS = 2


@pytest.fixture
def conn():
    c = queue.connect(os.environ["DATABASE_URL"])
    c.execute(SCHEMA_PATH.read_text())
    c.execute("TRUNCATE jobs, job_execution_logs")
    yield c
    c.close()


def start_worker(log_dir: Path, n: int) -> subprocess.Popen:
    env = {
        **os.environ,
        "LEASE_SECONDS": str(LEASE_SECONDS),
        "WORK_MIN_MS": "10",
        "WORK_MAX_MS": "40",
        "FAIL_RATE": "0.05",
        "POLL_INTERVAL_MS": "20",
    }
    log = open(log_dir / f"worker-{n}.log", "w")
    return subprocess.Popen(
        [sys.executable, "-m", "worker.main"], cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT
    )


def counts(conn) -> dict:
    rows = conn.execute("SELECT status, COUNT(*) AS n FROM jobs GROUP BY status").fetchall()
    return {r["status"]: r["n"] for r in rows}


def test_workers_drain_queue_and_survive_kill_9(conn, tmp_path):
    # The oldest job is slow (but shorter than the lease), so it is claimed
    # first and still running when we kill its worker. Its recovery then has
    # to come from the lease.
    victim_job = conn.execute(
        "INSERT INTO jobs (idempotency_key, payload) VALUES ('e2e-slow', '{\"work_ms\": 1500}') RETURNING id"
    ).fetchone()["id"]
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO jobs (idempotency_key) VALUES (%s)", [(f"e2e-{i}",) for i in range(N_JOBS)]
        )

    workers = [start_worker(tmp_path, n) for n in range(N_WORKERS)]
    try:
        victim = None
        deadline = time.monotonic() + 15
        while victim is None and time.monotonic() < deadline:
            row = conn.execute(
                "SELECT worker_id FROM jobs WHERE id = %s AND status = 'running'", (victim_job,)
            ).fetchone()
            if row:
                pid = int(row["worker_id"].split("-")[-2])
                victim = next(w for w in workers if w.pid == pid)
            time.sleep(0.05)
        assert victim is not None, "slow job was never claimed"
        victim.send_signal(signal.SIGKILL)
        victim.wait()

        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            c = counts(conn)
            if not c.get("queued") and not c.get("running"):
                break
            time.sleep(0.2)
        else:
            pytest.fail(f"queue not drained: {counts(conn)}")

        # SIGTERM: survivors finish their current job and exit cleanly.
        survivors = [w for w in workers if w is not victim]
        for w in survivors:
            w.send_signal(signal.SIGTERM)
        assert [w.wait(timeout=10) for w in survivors] == [0] * len(survivors)
    except BaseException:
        for f in sorted(tmp_path.glob("worker-*.log")):
            print(f"--- {f.name}\n{f.read_text()[-3000:]}")
        raise
    finally:
        for w in workers:
            if w.poll() is None:
                w.kill()

    c = counts(conn)
    assert sum(c.values()) == N_JOBS + 1
    assert set(c) <= {"succeeded", "failed"}
    assert c.get("failed", 0) > 0, "FAIL_RATE should have produced some failures"

    duplicated = conn.execute(
        "SELECT job_id FROM job_execution_logs GROUP BY job_id HAVING COUNT(*) > 1"
    ).fetchall()
    assert duplicated == []

    mismatched = conn.execute(
        """
        SELECT j.id, j.status, COUNT(l.id) AS effects
        FROM jobs j LEFT JOIN job_execution_logs l ON l.job_id = j.id
        GROUP BY j.id, j.status
        HAVING (j.status = 'succeeded') <> (COUNT(l.id) = 1)
        """
    ).fetchall()
    assert mismatched == []

    recovered = conn.execute("SELECT * FROM jobs WHERE id = %s", (victim_job,)).fetchone()
    assert recovered["attempts"] == 2
    assert f"-{victim.pid}-" not in recovered["worker_id"]
    assert recovered["status"] in ("succeeded", "failed")
