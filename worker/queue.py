"""SQL for the worker side of the queue.

Every function takes an open psycopg connection in autocommit mode. All time
comparisons use the database clock (NOW()); the worker's own clock is never
used for leases.
"""
import psycopg
from psycopg.rows import dict_row

CLAIM_SQL = """
UPDATE jobs
SET status = 'running',
    worker_id = %(worker_id)s,
    lease_expires_at = NOW() + make_interval(secs => %(lease_seconds)s),
    attempts = attempts + 1
WHERE id = (
    SELECT id FROM jobs
    WHERE status = 'queued'
       OR (status = 'running' AND lease_expires_at < NOW() AND attempts < %(max_attempts)s)
    ORDER BY created_at
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING id, payload, attempts
"""

# Guarded by the fencing token (worker_id + attempts) and the lease. If the job
# was cancelled, reclaimed by another worker, or our lease ran out, this
# matches 0 rows and nothing is written.
FINISH_SQL = """
UPDATE jobs
SET status = %(status)s,
    result_output = %(output)s,
    last_error = %(error)s,
    finished_at = NOW(),
    lease_expires_at = NULL
WHERE id = %(id)s
  AND status = 'running'
  AND worker_id = %(worker_id)s
  AND attempts = %(attempt)s
  AND lease_expires_at > NOW()
RETURNING id
"""

SIDE_EFFECT_SQL = """
INSERT INTO job_execution_logs (job_id, worker_id, attempt)
VALUES (%(id)s, %(worker_id)s, %(attempt)s)
"""

# Jobs whose worker died on every one of max_attempts claims. The claim query
# skips them, so without this they would sit in 'running' forever.
GIVE_UP_SQL = """
UPDATE jobs
SET status = 'failed',
    last_error = 'gave up: lease expired on every one of ' || attempts || ' attempts',
    finished_at = NOW(),
    lease_expires_at = NULL
WHERE status = 'running' AND lease_expires_at < NOW() AND attempts >= %(max_attempts)s
RETURNING id
"""


def connect(database_url: str) -> psycopg.Connection:
    return psycopg.connect(database_url, autocommit=True, row_factory=dict_row)


def claim(conn: psycopg.Connection, worker_id: str, lease_seconds: float, max_attempts: int) -> dict | None:
    """Atomically take the oldest available job, or return None."""
    return conn.execute(
        CLAIM_SQL,
        {"worker_id": worker_id, "lease_seconds": lease_seconds, "max_attempts": max_attempts},
    ).fetchone()


def finish(
    conn: psycopg.Connection,
    job: dict,
    worker_id: str,
    status: str,
    output: str | None = None,
    error: str | None = None,
) -> bool:
    """Record the outcome of an attempt. Returns False if we no longer own the job.

    For 'succeeded' the side-effect row is inserted in the same transaction as
    the status change, so either both commit or neither does.
    """
    params = {
        "id": job["id"],
        "worker_id": worker_id,
        "attempt": job["attempts"],
        "status": status,
        "output": output,
        "error": error,
    }
    with conn.transaction():
        if conn.execute(FINISH_SQL, params).fetchone() is None:
            return False
        if status == "succeeded":
            conn.execute(SIDE_EFFECT_SQL, params)
    return True


def give_up_exhausted(conn: psycopg.Connection, max_attempts: int) -> list:
    """Mark jobs that ran out of attempts as failed. Returns their ids."""
    return [row["id"] for row in conn.execute(GIVE_UP_SQL, {"max_attempts": max_attempts})]
