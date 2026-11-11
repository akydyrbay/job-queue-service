import uuid

import anyio
import pytest

from app.db import pool

pytestmark = pytest.mark.anyio


async def set_running(job_id: str) -> None:
    """Put a job into 'running' the way a worker claim would (no worker exists yet)."""
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE jobs SET status = 'running', worker_id = 'test-worker', "
            "lease_expires_at = NOW() + INTERVAL '10 seconds', attempts = attempts + 1 "
            "WHERE id = %s",
            (job_id,),
        )


async def set_status(job_id: str, status: str) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE jobs SET status = %s, finished_at = NOW() WHERE id = %s", (status, job_id)
        )


async def count_jobs(key: str | None = None) -> int:
    async with pool.connection() as conn:
        if key is None:
            cur = await conn.execute("SELECT COUNT(*) AS n FROM jobs")
        else:
            cur = await conn.execute(
                "SELECT COUNT(*) AS n FROM jobs WHERE idempotency_key = %s", (key,)
            )
        return (await cur.fetchone())["n"]


# --- create ---------------------------------------------------------------

async def test_create_job(client):
    r = await client.post("/jobs", json={"idempotency_key": "k1", "payload": {"lr": 0.01}})
    assert r.status_code == 201
    job = r.json()
    assert job["status"] == "queued"
    assert job["payload"] == {"lr": 0.01}
    assert job["attempts"] == 0
    assert job["worker_id"] is None


async def test_duplicate_post_returns_same_job(client):
    body = {"idempotency_key": "k1", "payload": {"a": 1}}
    first = await client.post("/jobs", json=body)
    second = await client.post("/jobs", json=body)
    assert first.status_code == 201
    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]
    assert await count_jobs() == 1


async def test_duplicate_key_with_different_payload_is_409(client):
    first = await client.post("/jobs", json={"idempotency_key": "k1", "payload": {"a": 1}})
    r = await client.post("/jobs", json={"idempotency_key": "k1", "payload": {"a": 2}})
    assert r.status_code == 409
    assert r.json()["detail"]["job_id"] == first.json()["id"]
    assert await count_jobs() == 1


async def test_payload_key_order_does_not_matter(client):
    await client.post("/jobs", json={"idempotency_key": "k1", "payload": {"a": 1, "b": 2}})
    r = await client.post("/jobs", json={"idempotency_key": "k1", "payload": {"b": 2, "a": 1}})
    assert r.status_code == 200


async def test_concurrent_posts_with_same_key_create_one_row(client):
    body = {"idempotency_key": "race", "payload": {"x": 1}}
    responses = []

    async def post():
        responses.append(await client.post("/jobs", json=body))

    async with anyio.create_task_group() as tg:
        for _ in range(50):
            tg.start_soon(post)

    assert await count_jobs("race") == 1
    assert {r.json()["id"] for r in responses} == {responses[0].json()["id"]}
    assert sorted(r.status_code for r in responses) == [200] * 49 + [201]


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"idempotency_key": ""},
        {"idempotency_key": "x" * 256},
        {"idempotency_key": "k", "payload": [1, 2]},
    ],
)
async def test_create_validation(client, body):
    r = await client.post("/jobs", json=body)
    assert r.status_code == 422


# --- get ------------------------------------------------------------------

async def test_get_job(client):
    created = (await client.post("/jobs", json={"idempotency_key": "k1"})).json()
    r = await client.get(f"/jobs/{created['id']}")
    assert r.status_code == 200
    assert r.json() == created


async def test_get_missing_job_is_404(client):
    r = await client.get(f"/jobs/{uuid.uuid4()}")
    assert r.status_code == 404


async def test_get_invalid_id_is_422(client):
    r = await client.get("/jobs/not-a-uuid")
    assert r.status_code == 422


# --- list -----------------------------------------------------------------

async def test_list_pagination(client):
    for i in range(25):
        await client.post("/jobs", json={"idempotency_key": f"k{i:02d}"})

    page1 = (await client.get("/jobs?page=1&limit=10")).json()
    page3 = (await client.get("/jobs?page=3&limit=10")).json()
    page4 = (await client.get("/jobs?page=4&limit=10")).json()

    assert page1["total"] == 25
    assert len(page1["items"]) == 10
    assert len(page3["items"]) == 5
    assert page4["items"] == []
    # newest first
    assert page1["items"][0]["idempotency_key"] == "k24"

    seen = []
    for page in range(1, 4):
        seen += [j["id"] for j in (await client.get(f"/jobs?page={page}&limit=10")).json()["items"]]
    assert len(seen) == len(set(seen)) == 25


async def test_list_filter_by_status(client):
    a = (await client.post("/jobs", json={"idempotency_key": "a"})).json()
    await client.post("/jobs", json={"idempotency_key": "b"})
    await client.post(f"/jobs/{a['id']}/cancel")

    r = (await client.get("/jobs?status=cancelled")).json()
    assert r["total"] == 1
    assert r["items"][0]["id"] == a["id"]


@pytest.mark.parametrize("query", ["page=0", "limit=0", "limit=101", "status=bogus"])
async def test_list_validation(client, query):
    r = await client.get(f"/jobs?{query}")
    assert r.status_code == 422


# --- cancel ---------------------------------------------------------------

async def test_cancel_queued_job(client):
    job = (await client.post("/jobs", json={"idempotency_key": "k1"})).json()
    r = await client.post(f"/jobs/{job['id']}/cancel")
    assert r.status_code == 200
    assert r.json()["status"] == "cancelled"
    assert r.json()["finished_at"] is not None


async def test_cancel_running_job(client):
    job = (await client.post("/jobs", json={"idempotency_key": "k1"})).json()
    await set_running(job["id"])
    r = await client.post(f"/jobs/{job['id']}/cancel")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "cancelled"
    assert body["lease_expires_at"] is None
    assert body["worker_id"] == "test-worker"  # kept for debugging


async def test_cancel_is_idempotent(client):
    job = (await client.post("/jobs", json={"idempotency_key": "k1"})).json()
    first = await client.post(f"/jobs/{job['id']}/cancel")
    second = await client.post(f"/jobs/{job['id']}/cancel")
    assert second.status_code == 200
    assert second.json() == first.json()


@pytest.mark.parametrize("final", ["succeeded", "failed"])
async def test_cancel_finished_job_is_409(client, final):
    job = (await client.post("/jobs", json={"idempotency_key": "k1"})).json()
    await set_status(job["id"], final)
    r = await client.post(f"/jobs/{job['id']}/cancel")
    assert r.status_code == 409
    assert (await client.get(f"/jobs/{job['id']}")).json()["status"] == final


async def test_cancel_missing_job_is_404(client):
    r = await client.post(f"/jobs/{uuid.uuid4()}/cancel")
    assert r.status_code == 404


async def test_duplicate_post_after_cancel_returns_cancelled_job(client):
    body = {"idempotency_key": "k1"}
    job = (await client.post("/jobs", json=body)).json()
    await client.post(f"/jobs/{job['id']}/cancel")
    r = await client.post("/jobs", json=body)
    assert r.status_code == 200
    assert r.json()["status"] == "cancelled"
    assert await count_jobs() == 1
