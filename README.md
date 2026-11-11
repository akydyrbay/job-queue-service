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
worker/
  main.py     worker loop: claim, simulate work, finish; SIGTERM handling
  queue.py    claim / guarded finish / give-up SQL
harness/
  harness.py  chaos test: 10k jobs, 8 workers, kill -9 every ~2 s, invariant checks
results/      raw output of every harness run, including failed ones
db/
  schema.sql  jobs and job_execution_logs tables, indexes, trigger (safe to re-run)
tests/
  test_api.py         endpoint tests against a real Postgres
  test_worker.py      claim/finish interleavings step by step (zombie, cancel, give-up)
  test_worker_e2e.py  4 worker processes drain 500 jobs, one killed with kill -9
  test_silent_failures.py  one test per silent failure, with negative controls
docs/
  design_note.md  one-page design: claiming, dead workers, cancellation, guarantee
  design.md       database choice, schema and concurrency decisions
  harness.md      what the chaos harness does, its invariants, and what it doesn't catch
  silent_failures.md  each silent failure: protection, how it was checked, evidence
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

## Workers

`docker compose up` starts 4 workers. Change the number with
`docker compose up --scale worker=8`. Locally: `python -m worker.main`.

| Env var | Default | Meaning |
| --- | --- | --- |
| `LEASE_SECONDS` | `10` | How long a claim is valid. A dead worker's job is picked up again after this. |
| `MAX_ATTEMPTS` | `5` | After this many claims whose lease ran out, the job is marked `failed`. |
| `FAIL_RATE` | `0` (`0.05` in compose) | Chance a simulated job fails. A failure is final; only crashes are retried. |
| `WORK_MIN_MS` / `WORK_MAX_MS` | `50` / `300` | Simulated job duration. |
| `POLL_INTERVAL_MS` | `100` | Idle sleep between claims (plus random jitter). |

A job payload can force behaviour: `{"work_ms": 1500}` sets the duration and
`{"fail": true}` always fails.

Each worker prints one line per event (`claimed`, `succeeded`, `failed`,
`stale_rejected`, `gave_up`, `db_error`), with its worker id and the job id.
SIGTERM lets the current job finish before the worker exits; `kill -9` is
handled by lease expiry.

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

## Chaos harness

```bash
docker compose up -d db api      # without the worker service; the harness runs its own 8 workers
python -m harness.harness --reset \
  --api-url http://localhost:8000 \
  --database-url postgresql://jobqueue:jobqueue@localhost:5432/jobqueue
```

Without Docker, add `--start-api` to run the API on a unix socket. Exit code 0
means every invariant passed. See [docs/harness.md](docs/harness.md) for the
details and [results/README.md](results/README.md) for every run so far.
