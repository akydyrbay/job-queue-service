# Job Queue Service

A training job queue built on FastAPI and PostgreSQL. Workers claim jobs with
`FOR UPDATE SKIP LOCKED` and lease timeouts.

## Layout

```
app/
  config.py   settings read from environment variables
  db.py       async Postgres connection pool, schema bootstrap
  main.py     FastAPI app, /health
db/
  schema.sql  jobs and job_execution_logs tables, indexes, trigger (safe to re-run)
docs/
  design.md   database choice, schema and concurrency decisions
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
