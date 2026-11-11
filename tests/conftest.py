import os

import pytest

# Tests TRUNCATE the tables, so they only run against an explicitly named
# test database, never whatever DATABASE_URL happens to point at.
TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
if not TEST_DATABASE_URL:
    pytest.skip("set TEST_DATABASE_URL to run the tests", allow_module_level=True)
os.environ["DATABASE_URL"] = TEST_DATABASE_URL

import httpx  # noqa: E402

from app.db import apply_schema, pool  # noqa: E402
from app.main import app  # noqa: E402


@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"


@pytest.fixture(scope="session")
async def db_pool(anyio_backend):
    await pool.open(wait=True)
    await apply_schema()
    yield pool
    await pool.close()


@pytest.fixture
async def client(db_pool):
    async with db_pool.connection() as conn:
        await conn.execute("TRUNCATE jobs, job_execution_logs")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
