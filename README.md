# Job Queue Service

A training job queue built on FastAPI and PostgreSQL. Workers claim jobs with
`FOR UPDATE SKIP LOCKED` and lease timeouts.

## Layout

```
app/
  config.py   settings read from environment variables
  db.py       async Postgres connection pool, schema bootstrap
  main.py     FastAPI app, /health
  models.py   request/response schemas
  repository.py  SQL for jobs
  routes/jobs.py job endpoints
db/
  schema.sql  jobs and job_execution_logs tables, indexes, trigger (safe to re-run)
tests/
  test_api.py endpoint tests against a real Postgres
docs/
  design_note.md  one-page design: claiming, dead workers, cancellation, guarantee
  design.md       database choice, schema and concurrency decisions
```

## Run

```bash
docker compose up --build
curl localhost:8000/health   # {"status":"ok","database":"ok"}
```

Without Docker (requires a running Postgres):

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export DATABASE_URL=postgresql://user:pass@localhost:5432/jobqueue
uvicorn app.main:app --reload
```

The API applies `db/schema.sql` on startup. The schema is safe to re-run.

## API

| Method | Path | Behaviour |
| --- | --- | --- |
| `POST` | `/jobs` | Body `{"idempotency_key": "...", "payload": {...}}`. `201` for a new job. `200` with the original job if the key was already used with the same payload. `409` if the key was used with a different payload. |
| `GET` | `/jobs/{id}` | The job, or `404`. |
| `GET` | `/jobs?page=1&limit=20&status=queued` | Newest first. `limit` is 1–100; `status` is optional. Returns `{items, page, limit, total}`. |
| `POST` | `/jobs/{id}/cancel` | Cancels a `queued` or `running` job. Cancelling an already cancelled job returns it unchanged (`200`). A `succeeded` or `failed` job returns `409`. |

## Tests

The tests run against a real Postgres and empty the tables before each test,
so they only run when `TEST_DATABASE_URL` is set. Point it at a database you
don't mind wiping.

```bash
pip install -r requirements.txt
docker compose up -d db
docker compose exec db createdb -U jobqueue jobqueue_test
TEST_DATABASE_URL=postgresql://jobqueue:jobqueue@localhost:5432/jobqueue_test pytest -q
```
