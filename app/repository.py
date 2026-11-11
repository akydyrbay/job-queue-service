from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from app.db import pool

JOB_COLUMNS = """
    id, idempotency_key, payload, status, worker_id, lease_expires_at, attempts,
    result_output, last_error, created_at, updated_at, finished_at
"""


async def create_job(idempotency_key: str, payload: dict[str, Any]) -> tuple[dict, bool]:
    """Insert a job, or return the existing one for this key.

    Returns (job, created). There is no "check, then insert" step: the UNIQUE
    constraint decides. A concurrent insert with the same key blocks on
    ON CONFLICT until the first one commits, then does nothing, and the
    SELECT (a new snapshot under READ COMMITTED) sees the committed row.
    """
    async with pool.connection() as conn:
        cur = await conn.execute(
            f"""
            INSERT INTO jobs (idempotency_key, payload) VALUES (%s, %s)
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING {JOB_COLUMNS}
            """,
            (idempotency_key, Jsonb(payload)),
        )
        row = await cur.fetchone()
        if row is not None:
            return row, True

        cur = await conn.execute(
            f"SELECT {JOB_COLUMNS} FROM jobs WHERE idempotency_key = %s",
            (idempotency_key,),
        )
        return await cur.fetchone(), False


async def get_job(job_id: UUID) -> dict | None:
    async with pool.connection() as conn:
        cur = await conn.execute(f"SELECT {JOB_COLUMNS} FROM jobs WHERE id = %s", (job_id,))
        return await cur.fetchone()


async def list_jobs(page: int, limit: int, status: str | None) -> tuple[list[dict], int]:
    where, params = ("WHERE status = %s", [status]) if status else ("", [])
    async with pool.connection() as conn:
        cur = await conn.execute(
            f"""
            SELECT {JOB_COLUMNS} FROM jobs {where}
            ORDER BY created_at DESC, id
            LIMIT %s OFFSET %s
            """,
            (*params, limit, (page - 1) * limit),
        )
        items = await cur.fetchall()
        cur = await conn.execute(f"SELECT COUNT(*) AS total FROM jobs {where}", params)
        total = (await cur.fetchone())["total"]
    return items, total


CANCEL_SQL = f"""
    UPDATE jobs
    SET status = 'cancelled', finished_at = NOW(), lease_expires_at = NULL
    WHERE id = %s AND status IN ('queued', 'running')
    RETURNING {JOB_COLUMNS}
"""


async def cancel_job(job_id: UUID) -> tuple[dict | None, bool]:
    """Cancel a queued or running job.

    Returns (job, cancelled_now). job is None if the id does not exist. If the
    UPDATE matches nothing the job is missing or already terminal, and terminal
    states never change, so the follow-up SELECT cannot race with anything.
    A running worker is not interrupted; its guarded completion update will
    match 0 rows because status is no longer 'running'.
    """
    async with pool.connection() as conn:
        cur = await conn.execute(CANCEL_SQL, (job_id,))
        row = await cur.fetchone()
        if row is not None:
            return row, True

        cur = await conn.execute(f"SELECT {JOB_COLUMNS} FROM jobs WHERE id = %s", (job_id,))
        return await cur.fetchone(), False
