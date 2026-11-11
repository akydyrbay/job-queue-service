from pathlib import Path

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from app.config import settings

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "db" / "schema.sql"
REQUIRED_TABLES = ("jobs", "job_execution_logs")

# Postgres default isolation level is READ COMMITTED, which is what the claim
# query (UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED)) relies on:
# each statement sees the latest committed rows and skips rows locked by others.
pool = AsyncConnectionPool(
    conninfo=settings.database_url,
    min_size=settings.db_pool_min_size,
    max_size=settings.db_pool_max_size,
    kwargs={"row_factory": dict_row},
    open=False,
)


async def apply_schema() -> None:
    """Apply db/schema.sql. Idempotent, so it is safe on every startup."""
    async with pool.connection() as conn:
        await conn.execute(SCHEMA_PATH.read_text())


async def missing_tables() -> list[str]:
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = ANY(%s)",
            (list(REQUIRED_TABLES),),
        )
        present = {row["table_name"] for row in await cur.fetchall()}
    return [t for t in REQUIRED_TABLES if t not in present]
