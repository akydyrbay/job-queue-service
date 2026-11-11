"""Worker queue operations against a real Postgres, one step at a time.

Each test drives claim/finish by hand so the interleaving is exact.
"""
import os

import pytest

from app.config import load_settings
from app.db import SCHEMA_PATH
from worker import queue
from worker.main import do_work, make_worker_id

LEASE = 10
MAX_ATTEMPTS = 5


@pytest.fixture
def conn():
    c = queue.connect(os.environ["DATABASE_URL"])
    c.execute(SCHEMA_PATH.read_text())
    c.execute("TRUNCATE jobs, job_execution_logs")
    yield c
    c.close()


def add_job(conn, key, payload="{}"):
    return conn.execute(
        "INSERT INTO jobs (idempotency_key, payload) VALUES (%s, %s) RETURNING id", (key, payload)
    ).fetchone()["id"]


def get(conn, job_id):
    return conn.execute("SELECT * FROM jobs WHERE id = %s", (job_id,)).fetchone()


def effects(conn, job_id):
    return conn.execute(
        "SELECT worker_id, attempt FROM job_execution_logs WHERE job_id = %s", (job_id,)
    ).fetchall()


def expire_lease(conn, job_id):
    conn.execute(
        "UPDATE jobs SET lease_expires_at = NOW() - INTERVAL '1 second' WHERE id = %s", (job_id,)
    )


def claim(conn, worker_id):
    return queue.claim(conn, worker_id, LEASE, MAX_ATTEMPTS)


# --- claim ----------------------------------------------------------------

def test_claim_takes_oldest_queued_job(conn):
    first = add_job(conn, "a")
    add_job(conn, "b")

    job = claim(conn, "w1")

    assert job["id"] == first
    assert job["attempts"] == 1
    row = get(conn, first)
    assert row["status"] == "running"
    assert row["worker_id"] == "w1"
    lease_left = conn.execute(
        "SELECT EXTRACT(EPOCH FROM lease_expires_at - NOW()) AS s FROM jobs WHERE id = %s", (first,)
    ).fetchone()["s"]
    assert LEASE - 1 < lease_left <= LEASE


def test_claim_returns_none_when_nothing_to_do(conn):
    assert claim(conn, "w1") is None


def test_each_job_claimed_once(conn):
    ids = {add_job(conn, k) for k in "abc"}
    claimed = {claim(conn, w)["id"] for w in ("w1", "w2", "w3")}
    assert claimed == ids
    assert claim(conn, "w4") is None


def test_claim_skips_cancelled_and_finished_jobs(conn):
    for key, status in [("c", "cancelled"), ("s", "succeeded"), ("f", "failed")]:
        conn.execute(
            "UPDATE jobs SET status = %s WHERE id = %s", (status, add_job(conn, key))
        )
    assert claim(conn, "w1") is None


def test_running_job_with_valid_lease_is_not_reclaimed(conn):
    add_job(conn, "a")
    claim(conn, "w1")
    assert claim(conn, "w2") is None


# --- finish ---------------------------------------------------------------

def test_success_writes_exactly_one_side_effect(conn):
    job_id = add_job(conn, "a")
    job = claim(conn, "w1")

    assert queue.finish(conn, job, "w1", "succeeded", output="ok")

    row = get(conn, job_id)
    assert row["status"] == "succeeded"
    assert row["result_output"] == "ok"
    assert row["lease_expires_at"] is None
    assert row["finished_at"] is not None
    assert effects(conn, job_id) == [{"worker_id": "w1", "attempt": 1}]


def test_failure_has_no_side_effect(conn):
    job_id = add_job(conn, "a")
    job = claim(conn, "w1")

    assert queue.finish(conn, job, "w1", "failed", error="boom")

    row = get(conn, job_id)
    assert row["status"] == "failed"
    assert row["last_error"] == "boom"
    assert effects(conn, job_id) == []


def test_finish_twice_is_rejected(conn):
    job_id = add_job(conn, "a")
    job = claim(conn, "w1")
    assert queue.finish(conn, job, "w1", "succeeded")
    assert not queue.finish(conn, job, "w1", "succeeded")
    assert len(effects(conn, job_id)) == 1


def test_cancelled_while_running_is_rejected(conn):
    job_id = add_job(conn, "a")
    job = claim(conn, "w1")
    conn.execute("UPDATE jobs SET status = 'cancelled', lease_expires_at = NULL WHERE id = %s", (job_id,))

    assert not queue.finish(conn, job, "w1", "succeeded")

    assert get(conn, job_id)["status"] == "cancelled"
    assert effects(conn, job_id) == []


def test_expired_lease_rejects_late_finish(conn):
    """Worker froze past its lease; nobody reclaimed it yet. Its write is still refused."""
    job_id = add_job(conn, "a")
    job = claim(conn, "w1")
    expire_lease(conn, job_id)

    assert not queue.finish(conn, job, "w1", "succeeded")

    assert get(conn, job_id)["status"] == "running"
    assert effects(conn, job_id) == []
    # and it is up for grabs again
    assert claim(conn, "w2")["id"] == job_id


def test_zombie_after_reclaim_is_rejected(conn):
    """w1 freezes, its lease expires, w2 reclaims and finishes, then w1 wakes up."""
    job_id = add_job(conn, "a")
    old = claim(conn, "w1")
    expire_lease(conn, job_id)
    new = claim(conn, "w2")
    assert new["attempts"] == 2

    assert queue.finish(conn, new, "w2", "succeeded")
    assert not queue.finish(conn, old, "w1", "succeeded")

    assert get(conn, job_id)["worker_id"] == "w2"
    assert effects(conn, job_id) == [{"worker_id": "w2", "attempt": 2}]


def test_same_worker_old_attempt_is_rejected(conn):
    """Fencing is by attempt, not just worker id: a stale attempt by the same worker loses."""
    job_id = add_job(conn, "a")
    old = claim(conn, "w1")
    expire_lease(conn, job_id)
    new = claim(conn, "w1")

    assert not queue.finish(conn, old, "w1", "succeeded")
    assert queue.finish(conn, new, "w1", "succeeded")
    assert effects(conn, job_id) == [{"worker_id": "w1", "attempt": 2}]


# --- giving up --------------------------------------------------------------

def test_job_is_reclaimed_until_max_attempts_then_failed(conn):
    job_id = add_job(conn, "a")
    for attempt in range(1, MAX_ATTEMPTS + 1):
        job = claim(conn, f"w{attempt}")
        assert job["attempts"] == attempt
        expire_lease(conn, job_id)

    assert claim(conn, "w-next") is None
    assert queue.give_up_exhausted(conn, MAX_ATTEMPTS) == [job_id]

    row = get(conn, job_id)
    assert row["status"] == "failed"
    assert "5 attempts" in row["last_error"]
    assert effects(conn, job_id) == []


def test_give_up_ignores_jobs_with_live_lease(conn):
    job_id = add_job(conn, "a")
    conn.execute(
        "UPDATE jobs SET status = 'running', worker_id = 'w1', attempts = %s, "
        "lease_expires_at = NOW() + INTERVAL '10 seconds' WHERE id = %s",
        (MAX_ATTEMPTS, job_id),
    )
    assert queue.give_up_exhausted(conn, MAX_ATTEMPTS) == []
    assert get(conn, job_id)["status"] == "running"


# --- do_work ----------------------------------------------------------------

def test_do_work_honours_payload_overrides():
    settings = load_settings()
    assert do_work({"payload": {"work_ms": 1}}, settings)[0] == "succeeded"
    assert do_work({"payload": {"work_ms": 1, "fail": True}}, settings) == (
        "failed", None, "simulated failure",
    )


def test_worker_ids_are_unique():
    assert len({make_worker_id() for _ in range(100)}) == 100
