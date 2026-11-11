# Design: Architecture and Database Schema

## Architecture

```
HTTP clients ──► FastAPI (app/) ──► PostgreSQL ◄── Worker 1..N
```

The database is the single source of truth. The API and the workers share no
in-memory state, so killing any process (`kill -9`) loses nothing that was
already committed.

## Why PostgreSQL instead of SQLite

The harness runs 8 workers claiming jobs while the API takes inserts,
duplicate POSTs and cancels at the same time.

- **SQLite** has one writer lock for the whole database. Every claim, insert
  and cancel would wait in line for it, and with 8 workers plus the API you
  get `database is locked` errors and busy-wait retries.
- **PostgreSQL** locks individual rows. `SELECT ... FOR UPDATE SKIP LOCKED`
  lets each worker skip rows another worker has locked and take the next free
  one, so claims run in parallel and never block API inserts.

## Schema decisions (`db/schema.sql`)

| Decision | Reason |
| --- | --- |
| `idempotency_key UNIQUE NOT NULL` | Duplicate POSTs are rejected by the database itself, so concurrent retries cannot race past an application level "does it exist?" check. |
| `payload JSONB` | Added to the guide's schema because `POST /jobs` accepts a payload. |
| `worker_id` + `lease_expires_at` | A claim is a lease, not ownership. If the worker dies, the lease runs out and another worker can reclaim the job. |
| Lease timestamps come from `NOW()` | Only the database clock is used, so worker clock skew cannot stretch or shrink a lease. |
| `attempts` | Increased on every claim. A value above 1 shows the job was reclaimed after a crash, which helps debug harness runs. |
| `CHECK running_has_lease` | A `running` row without an owner or lease could never be recovered and would sit in `running` forever. The database refuses to store one. |
| `job_execution_logs` **not** unique on `job_id` | This table is how the harness detects duplicate side effects. A UNIQUE constraint would turn a double execution into an error that hides the bug. |
| `job_execution_logs.attempt` | Ties each side effect to the claim that produced it. |
| `updated_at` trigger | Kept correct for every update, whichever code path makes it. |

### Indexes

| Index | Used by |
| --- | --- |
| `idx_jobs_queued (created_at) WHERE status='queued'` | Claim path: oldest queued job first. A partial index only contains pending rows, so it stays small as finished jobs pile up. |
| `idx_jobs_running_lease (lease_expires_at) WHERE status='running'` | Claim path: finding expired leases (dead-worker recovery). |
| `idx_jobs_created_at (created_at DESC, id)` | `GET /jobs` pagination with a stable sort order. |
| `idx_job_execution_logs_job_id` | Invariant check `GROUP BY job_id HAVING COUNT(*) > 1`. |

These two partial indexes replace the guide's single
`(status, lease_expires_at)` index. That index cannot serve
`ORDER BY created_at` for queued jobs, and it also holds every running row.

## Concurrency model

- **Isolation:** Postgres' default, `READ COMMITTED`. The claim is a single
  statement, `UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1)`.
  It locks the row it chooses, so two workers cannot claim the same job.
- **Avoiding duplicate side effects:** the worker inserts the
  `job_execution_logs` row and updates the job to `succeeded` in one
  transaction. The update is guarded:
  `WHERE id = :id AND status = 'running' AND worker_id = :me AND lease_expires_at > NOW()`.
  If that update matches 0 rows, the transaction rolls back and the log row is
  never committed. This covers a worker that resumes after its lease expired
  and a job cancelled while it was running.
- **Delivery guarantee:** jobs are claimed at least once. The side effect is
  committed at most once per job because of the guarded transaction above.

## Configuration

| Env var | Default | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql://jobqueue:jobqueue@localhost:5432/jobqueue` | Postgres DSN |
| `DB_POOL_MIN_SIZE` / `DB_POOL_MAX_SIZE` | `1` / `10` | Connection pool size per process |
| `LEASE_SECONDS` | `10` | Lease length used by workers |
