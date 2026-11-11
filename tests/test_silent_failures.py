"""One test per silent failure from the task, plus negative controls.

Each section shows the protection working and, where it can be done
deterministically, what happens without it, so a passing test means
something. Evidence and discussion: docs/silent_failures.md.
"""
import asyncio
import os
import random
import re
import signal
import subprocess
import sys
import threading
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
import pytest
from psycopg.rows import dict_row

from app.db import SCHEMA_PATH, pool
from app.repository import CANCEL_SQL
from worker import queue

ROOT = Path(__file__).resolve().parent.parent
LEASE = 10
MAX_ATTEMPTS = 5


@pytest.fixture
def connect():
    """Factory for extra connections; all are closed after the test."""
    conns = []

    def make(autocommit=True):
        c = psycopg.connect(os.environ["DATABASE_URL"], autocommit=autocommit, row_factory=dict_row)
        conns.append(c)
        return c

    setup = make()
    setup.execute(SCHEMA_PATH.read_text())
    setup.execute("TRUNCATE jobs, job_execution_logs")
    yield make
    for c in conns:
        c.close()


def add_job(conn, key, payload="{}"):
    return conn.execute(
        "INSERT INTO jobs (idempotency_key, payload) VALUES (%s, %s) RETURNING id", (key, payload)
    ).fetchone()["id"]


def job(conn, job_id):
    return conn.execute("SELECT * FROM jobs WHERE id = %s", (job_id,)).fetchone()


def effects(conn, job_id):
    return conn.execute(
        "SELECT worker_id, attempt FROM job_execution_logs WHERE job_id = %s", (job_id,)
    ).fetchall()


def wait_for(predicate, timeout=10, interval=0.05, what="condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(interval)
    raise AssertionError(f"timed out waiting for {what}")


def start_worker(tmp_path, name, extra_env=None):
    env = {**os.environ, "LEASE_SECONDS": "2", "POLL_INTERVAL_MS": "20", **(extra_env or {})}
    log_path = tmp_path / f"{name}.log"
    proc = subprocess.Popen(
        [sys.executable, "-m", "worker.main"], cwd=ROOT, env=env,
        stdout=open(log_path, "w"), stderr=subprocess.STDOUT,
    )
    return proc, log_path


def stop(*procs):
    for p in procs:
        if p.poll() is None:
            p.send_signal(signal.SIGCONT)
            p.terminate()
    for p in procs:
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()


# =============================================================================
# 1. Double claim
# =============================================================================

def test_double_claim_locked_row_is_skipped_not_shared(connect):
    """Worker A has claimed j1 but not committed yet. B must get j2, without waiting."""
    setup = connect()
    j1, j2 = add_job(setup, "j1"), add_job(setup, "j2")

    a = connect(autocommit=False)
    claimed_a = queue.claim(a, "A", LEASE, MAX_ATTEMPTS)  # row lock held, txn open

    b = connect()
    b.execute("SET statement_timeout = '1s'")  # raises if B blocks on A's lock
    claimed_b = queue.claim(b, "B", LEASE, MAX_ATTEMPTS)
    a.commit()

    assert claimed_a["id"] == j1
    assert claimed_b["id"] == j2


def test_double_claim_16_threads_each_job_claimed_once(connect):
    setup = connect()
    with setup.cursor() as cur:
        cur.executemany("INSERT INTO jobs (idempotency_key) VALUES (%s)", [(f"k{i}",) for i in range(500)])

    claims = []
    lock = threading.Lock()
    start = threading.Barrier(16)

    def worker(n):
        conn = connect()
        start.wait()
        while (j := queue.claim(conn, f"w{n}", LEASE, MAX_ATTEMPTS)) is not None:
            with lock:
                claims.append(j["id"])

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(claims) == 500
    assert len(set(claims)) == 500
    assert setup.execute("SELECT MAX(attempts) AS m FROM jobs").fetchone()["m"] == 1


def test_negative_control_naive_claim_double_claims(connect):
    """SELECT-then-UPDATE without a row lock: both workers see the job as free and both take it."""
    setup = connect()
    j1 = add_job(setup, "j1")
    a, b = connect(), connect()
    naive_select = "SELECT id FROM jobs WHERE status = 'queued' ORDER BY created_at LIMIT 1"
    naive_update = ("UPDATE jobs SET status = 'running', worker_id = %s, attempts = attempts + 1, "
                    "lease_expires_at = NOW() + INTERVAL '10 seconds' WHERE id = %s RETURNING id")

    seen_a = a.execute(naive_select).fetchone()["id"]
    seen_b = b.execute(naive_select).fetchone()["id"]
    got_a = a.execute(naive_update, ("A", seen_a)).fetchone()
    got_b = b.execute(naive_update, ("B", seen_b)).fetchone()

    assert got_a["id"] == got_b["id"] == j1  # both "own" the same job
    assert job(setup, j1)["attempts"] == 2


# =============================================================================
# 2. Zombie worker: froze, lease expired, came back and tried to write
# =============================================================================

def test_frozen_worker_process_cannot_write_after_lease_expired(connect, tmp_path):
    """Real processes. Worker A is frozen with SIGSTOP mid-job (like a 5-minute GC
    pause or a suspended VM). Its lease expires, worker B takes over and finishes.
    Then A is resumed with SIGCONT and tries to finish the job it no longer owns."""
    setup = connect()
    job_id = add_job(setup, "slow", '{"work_ms": 1000}')

    a, a_log = start_worker(tmp_path, "A")
    b = None
    try:
        row = wait_for(lambda: (r := job(setup, job_id))["status"] == "running" and r, what="A to claim")
        assert f"-{a.pid}-" in row["worker_id"]
        a.send_signal(signal.SIGSTOP)

        wait_for(lambda: setup.execute(
            "SELECT lease_expires_at < NOW() AS expired FROM jobs WHERE id = %s", (job_id,)
        ).fetchone()["expired"], what="A's lease to expire")

        b, b_log = start_worker(tmp_path, "B")
        wait_for(lambda: job(setup, job_id)["status"] == "succeeded", what="B to finish")

        a.send_signal(signal.SIGCONT)
        wait_for(lambda: "event=stale_rejected" in a_log.read_text(), what="A's late write to be rejected")
    finally:
        stop(a, *([b] if b else []))

    final = job(setup, job_id)
    assert final["status"] == "succeeded"
    assert f"-{b.pid}-" in final["worker_id"]
    assert final["attempts"] == 2
    assert len(effects(setup, job_id)) == 1
    assert effects(setup, job_id)[0]["attempt"] == 2
    assert f"event=stale_rejected worker={row['worker_id']} job={job_id} attempt=1" in a_log.read_text()


def test_negative_control_unguarded_finish_lets_zombie_overwrite(connect):
    """Without the worker_id/attempts/lease check, the zombie's late write wins
    and the job gets a second side effect."""
    setup = connect()
    job_id = add_job(setup, "j")
    zombie = queue.claim(setup, "A", LEASE, MAX_ATTEMPTS)
    setup.execute("UPDATE jobs SET lease_expires_at = NOW() - INTERVAL '1 second' WHERE id = %s", (job_id,))
    current = queue.claim(setup, "B", LEASE, MAX_ATTEMPTS)
    assert queue.finish(setup, current, "B", "succeeded", output="B's result")

    # what an unguarded finish would do
    setup.execute("UPDATE jobs SET status = 'succeeded', result_output = 'A''s stale result' WHERE id = %s",
                  (job_id,))
    setup.execute("INSERT INTO job_execution_logs (job_id, worker_id, attempt) VALUES (%s, 'A', %s)",
                  (job_id, zombie["attempts"]))

    assert job(setup, job_id)["result_output"] == "A's stale result"
    assert len(effects(setup, job_id)) == 2


# =============================================================================
# 3. Cancel / complete race
# =============================================================================

def test_cancel_and_complete_racing_never_mix(connect):
    """Cancel and finish released together, 300 times. Exactly one may win, and
    the final state must match: cancelled with 0 effects, or succeeded with 1.

    finish() needs four round trips (BEGIN, UPDATE, INSERT, COMMIT) and cancel
    one, so with a bare barrier cancel won all 300 times and the "finish wins"
    path was never tested. A random 0-3 ms delay on each side makes both
    orders happen, including ones where the two transactions overlap."""
    setup = connect()
    finisher, canceller = connect(), connect()
    outcomes = Counter()

    for i in range(300):
        job_id = add_job(setup, f"race-{i}")
        claimed = queue.claim(setup, "w", LEASE, MAX_ATTEMPTS)
        barrier = threading.Barrier(2)
        result = {}

        def finish():
            barrier.wait()
            time.sleep(random.uniform(0, 0.003))
            result["finished"] = queue.finish(finisher, claimed, "w", "succeeded")

        def cancel():
            barrier.wait()
            time.sleep(random.uniform(0, 0.003))
            result["cancelled"] = canceller.execute(CANCEL_SQL, (job_id,)).fetchone() is not None

        threads = [threading.Thread(target=finish), threading.Thread(target=cancel)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        final = job(setup, job_id)
        n_effects = len(effects(setup, job_id))
        assert result["finished"] != result["cancelled"], f"iteration {i}: both or neither won: {result}"
        if result["finished"]:
            assert (final["status"], n_effects) == ("succeeded", 1), f"iteration {i}"
            outcomes["finish won"] += 1
        else:
            assert (final["status"], n_effects) == ("cancelled", 0), f"iteration {i}"
            outcomes["cancel won"] += 1

    print(f"\ncancel/complete race outcomes over 300 runs: {dict(outcomes)}")
    # Both paths must have been exercised, otherwise this test proves nothing.
    assert outcomes["finish won"] > 0 and outcomes["cancel won"] > 0, dict(outcomes)


def test_negative_control_finish_without_status_check_overrides_cancel(connect):
    """If finish only checked ownership and not status='running', a cancel the API
    already confirmed with 200 would be silently undone."""
    setup = connect()
    job_id = add_job(setup, "j")
    queue.claim(setup, "w", LEASE, MAX_ATTEMPTS)
    assert setup.execute(CANCEL_SQL, (job_id,)).fetchone() is not None  # API says "cancelled"

    setup.execute("UPDATE jobs SET status = 'succeeded' WHERE id = %s AND worker_id = 'w'", (job_id,))

    assert job(setup, job_id)["status"] == "succeeded"


# =============================================================================
# 4. Duplicate on request retry
# =============================================================================

@pytest.mark.anyio
async def test_retry_while_first_insert_is_uncommitted_returns_the_same_job(client):
    """The first request's INSERT is still in an open transaction when the retry
    arrives. The retry must wait for it, then return that job, not create a second."""
    async with await psycopg.AsyncConnection.connect(os.environ["DATABASE_URL"], row_factory=dict_row) as first:
        cur = await first.execute(
            "INSERT INTO jobs (idempotency_key, payload) VALUES ('retry', '{}') RETURNING id"
        )
        first_id = str((await cur.fetchone())["id"])

        retry = asyncio.create_task(client.post("/jobs", json={"idempotency_key": "retry", "payload": {}}))
        await asyncio.sleep(0.5)
        assert not retry.done(), "retry should be waiting on the uncommitted insert"

        await first.commit()
        r = await retry

    assert r.status_code == 200
    assert r.json()["id"] == first_id
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT COUNT(*) AS n FROM jobs WHERE idempotency_key = 'retry'")
        assert (await cur.fetchone())["n"] == 1


@pytest.mark.anyio
async def test_retry_after_first_insert_rolled_back_creates_the_job(client):
    async with await psycopg.AsyncConnection.connect(os.environ["DATABASE_URL"]) as first:
        await first.execute("INSERT INTO jobs (idempotency_key, payload) VALUES ('retry', '{}')")
        retry = asyncio.create_task(client.post("/jobs", json={"idempotency_key": "retry", "payload": {}}))
        await asyncio.sleep(0.5)
        assert not retry.done()
        await first.rollback()
        r = await retry

    assert r.status_code == 201


@pytest.mark.anyio
async def test_100_concurrent_posts_with_one_key(client):
    body = {"idempotency_key": "burst", "payload": {"x": 1}}
    responses = await asyncio.gather(*(client.post("/jobs", json=body) for _ in range(100)))

    assert len({r.json()["id"] for r in responses}) == 1
    assert Counter(r.status_code for r in responses) == {201: 1, 200: 99}


# =============================================================================
# 5. Transaction isolation level
# =============================================================================

@pytest.mark.anyio
async def test_api_and_worker_connections_use_read_committed(client):
    async with pool.connection() as conn:
        cur = await conn.execute("SHOW transaction_isolation")
        assert (await cur.fetchone())["transaction_isolation"] == "read committed"
    worker_conn = queue.connect(os.environ["DATABASE_URL"])
    try:
        assert worker_conn.execute("SHOW transaction_isolation").fetchone()["transaction_isolation"] \
            == "read committed"
    finally:
        worker_conn.close()


def test_claim_under_repeatable_read_fails_instead_of_skipping(connect):
    """Under REPEATABLE READ, a worker whose snapshot predates another worker's
    claim gets a serialization error instead of moving on to the next job.
    It would need retry logic, and under load most claims would collide."""
    setup = connect()
    add_job(setup, "j1")
    j2 = add_job(setup, "j2")

    b = connect(autocommit=False)
    b.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
    b.execute("SELECT 1 FROM jobs LIMIT 1")  # snapshot taken now

    queue.claim(connect(), "A", LEASE, MAX_ATTEMPTS)  # A claims j1 and commits

    with pytest.raises(psycopg.errors.SerializationFailure):
        queue.claim(b, "B", LEASE, MAX_ATTEMPTS)
    b.rollback()

    # Same sequence under READ COMMITTED (what the code uses): B just takes j2.
    c = connect(autocommit=False)
    c.execute("SELECT 1 FROM jobs LIMIT 1")
    assert queue.claim(c, "C", LEASE, MAX_ATTEMPTS)["id"] == j2
    c.commit()


def test_negative_control_check_then_insert_duplicates_under_read_committed(connect):
    """Why idempotency is a UNIQUE constraint and not an `if not exists` check:
    under READ COMMITTED both requests see "no row" and both insert."""
    setup = connect()
    setup.execute("CREATE TABLE IF NOT EXISTS naive_jobs (idempotency_key TEXT)")
    setup.execute("TRUNCATE naive_jobs")
    a, b = connect(autocommit=False), connect(autocommit=False)
    try:
        exists = "SELECT 1 FROM naive_jobs WHERE idempotency_key = 'k'"
        assert a.execute(exists).fetchone() is None
        assert b.execute(exists).fetchone() is None
        a.execute("INSERT INTO naive_jobs VALUES ('k')")
        b.execute("INSERT INTO naive_jobs VALUES ('k')")
        a.commit()
        b.commit()
        assert setup.execute("SELECT COUNT(*) AS n FROM naive_jobs").fetchone()["n"] == 2

        # The same check-then-insert against the real table fails loudly instead.
        assert a.execute("SELECT 1 FROM jobs WHERE idempotency_key = 'k'").fetchone() is None
        assert b.execute("SELECT 1 FROM jobs WHERE idempotency_key = 'k'").fetchone() is None
        a.execute("INSERT INTO jobs (idempotency_key) VALUES ('k')")
        a.commit()
        with pytest.raises(psycopg.errors.UniqueViolation):
            b.execute("INSERT INTO jobs (idempotency_key) VALUES ('k')")
        b.rollback()
    finally:
        setup.execute("DROP TABLE naive_jobs")


# =============================================================================
# 6. Clock skew
# =============================================================================

WALL_CLOCK = re.compile(r"time\.time\(|datetime\.(now|utcnow|today)\(|date\.today\(")


def test_no_wall_clock_reads_in_service_code():
    """Leases must come from the database's NOW(). time.monotonic is fine: it is
    only used to space out the give-up check, never compared with the database."""
    offenders = [
        f"{path.relative_to(ROOT)}:{n}: {line.strip()}"
        for path in [*ROOT.glob("app/**/*.py"), *ROOT.glob("worker/**/*.py")]
        for n, line in enumerate(path.read_text().splitlines(), 1)
        if WALL_CLOCK.search(line)
    ]
    assert offenders == []


@pytest.mark.parametrize("offset_hours", [+1, -1])
def test_worker_with_skewed_clock_gets_a_normal_lease(connect, tmp_path, offset_hours):
    """Run a real worker whose time.time() is off by an hour. The lease it gets is
    still NOW() + LEASE_SECONDS on the database clock, and the job completes."""
    sitecustomize = tmp_path / "sitecustomize.py"
    marker = tmp_path / "clock_was_skewed"
    sitecustomize.write_text(
        "import os, time\n"
        "_real = time.time\n"
        "_offset = float(os.environ['FAKE_CLOCK_OFFSET'])\n"
        "time.time = lambda: _real() + _offset\n"
        f"open({str(marker)!r}, 'w').write(str(time.time() - _real()))\n"
    )
    setup = connect()
    job_id = add_job(setup, "skewed", '{"work_ms": 1500}')
    proc, _ = start_worker(tmp_path, "skewed", {
        "PYTHONPATH": str(tmp_path), "FAKE_CLOCK_OFFSET": str(offset_hours * 3600),
    })
    try:
        lease_left = wait_for(lambda: setup.execute(
            "SELECT EXTRACT(EPOCH FROM lease_expires_at - NOW()) AS s FROM jobs "
            "WHERE id = %s AND status = 'running'", (job_id,)
        ).fetchone(), what="claim")["s"]
        wait_for(lambda: job(setup, job_id)["status"] == "succeeded", what="job to finish")
    finally:
        stop(proc)

    assert abs(float(marker.read_text()) - offset_hours * 3600) < 1  # the skew was really active
    assert 0 < lease_left <= 2
    assert len(effects(setup, job_id)) == 1


def test_negative_control_lease_from_worker_clock_is_reclaimed_immediately(connect):
    """If the worker computed its own lease and its clock were an hour slow, the
    lease would already be in the past: another worker reclaims a job that is
    still running, and it runs twice."""
    setup = connect()
    job_id = add_job(setup, "j")
    slow_clock_now = datetime.now(timezone.utc) - timedelta(hours=1)
    setup.execute(
        "UPDATE jobs SET status = 'running', worker_id = 'slow-clock', attempts = 1, "
        "lease_expires_at = %s WHERE id = %s",
        (slow_clock_now + timedelta(seconds=LEASE), job_id),
    )

    stolen = queue.claim(setup, "B", LEASE, MAX_ATTEMPTS)

    assert stolen["id"] == job_id and stolen["attempts"] == 2


# =============================================================================
# Extra: worker dies in the middle of its finishing transaction
# =============================================================================

def test_crash_between_status_update_and_commit_leaves_no_trace(connect):
    """The side effect and the status change are one transaction. If the worker
    dies after both statements but before COMMIT, Postgres rolls both back, and
    the job is recovered via its lease like any other crash."""
    setup = connect()
    job_id = add_job(setup, "j")
    claimed = queue.claim(setup, "w", LEASE, MAX_ATTEMPTS)

    dying = connect(autocommit=False)
    params = {"id": job_id, "worker_id": "w", "attempt": claimed["attempts"],
              "status": "succeeded", "output": None, "error": None}
    assert dying.execute(queue.FINISH_SQL, params).fetchone() is not None
    dying.execute(queue.SIDE_EFFECT_SQL, params)
    dying.pgconn.finish()  # connection gone without COMMIT, like kill -9

    row = job(setup, job_id)
    assert row["status"] == "running"
    assert effects(setup, job_id) == []
