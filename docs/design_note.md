# Design Note

I wrote this before the API and worker code. Schema details are in
[design.md](design.md), this note covers how the queue behaves when things go
wrong.

## Storage

Postgres. With 8 workers claiming jobs while the API takes inserts, retries and
cancels at the same time, SQLite's single writer lock would make everything
wait in line. Postgres locks individual rows and has `SKIP LOCKED`, which is
what a queue table needs.

## How a worker claims a job

One statement, under the default `READ COMMITTED`:

```sql
UPDATE jobs
SET status = 'running', worker_id = :me,
    lease_expires_at = NOW() + :lease, attempts = attempts + 1
WHERE id = (
    SELECT id FROM jobs
    WHERE status = 'queued'
       OR (status = 'running' AND lease_expires_at < NOW())
    ORDER BY created_at
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING *;
```

The inner select locks the row it picks, and other workers skip locked rows
instead of waiting on them, so two workers can't take the same job. The
worker remembers the `attempts` value it got back. That number is its
**fencing token**: each claim increments it, so a newer claim always makes an
older token stale.

## How I detect a dead worker

I don't detect it directly. A worker holds a lease (10 s by default, while a
simulated job takes 50–300 ms). If it dies, the lease expires, and the same
claim query picks the job up again, because it also matches `running` rows
whose lease has passed. `kill -9` needs no cleanup code. All times come from
the database's `NOW()`, so a worker's local clock never matters.

To stop one bad job from killing workers forever, a job that has been claimed
`MAX_ATTEMPTS` (5) times is marked `failed` instead of being reclaimed again.

I'm not using heartbeats, because jobs are much shorter than the lease. If
jobs could run longer than the lease, I'd need them; see the weak spots below.

## Finishing a job

The side effect is a row in `job_execution_logs`. The worker writes it in the
**same transaction** as the status change, and only if it still owns the job:

```sql
UPDATE jobs SET status = 'succeeded', finished_at = NOW(), ...
WHERE id = :id AND status = 'running'
  AND worker_id = :me AND attempts = :my_attempt
  AND lease_expires_at > NOW();
-- 1 row  -> insert the execution log row, commit
-- 0 rows -> roll back: the job was cancelled, reclaimed, or my lease ran out
```

Marking a job `failed` uses the same guard.

Something I had to think about: in Postgres, `NOW()` is the time the
*transaction started*, so the lease check can be slightly stale. Correctness
doesn't depend on it, though. If another worker reclaims the job at the same
moment, the two updates are serialized by the row lock. Postgres then
re-checks my `WHERE` against the newer row, sees a different
`attempts`/`worker_id`, and updates 0 rows. The lease is what keeps jobs
moving after a crash; the fencing token is what keeps results correct.

## What happens when a running job is cancelled

`POST /jobs/{id}/cancel` sets `cancelled` only if the job is `queued` or
`running`. On a finished job it returns 409, and on an already-cancelled job
it returns the job unchanged. The worker isn't interrupted. It finishes its
simulated work, then its guarded update finds `status = 'cancelled'`, matches
0 rows, and rolls back, so a cancelled job never gets a side effect. If the
cancel and the completion race, the row lock decides which one wins: the
result is either `succeeded` with one side effect or `cancelled` with none,
never a mix.

## Guarantee

**At-least-once execution, exactly-once effect.** A job can be *worked on*
more than once: a worker can freeze or die after doing the work but before
committing. Only one attempt can ever commit its side effect, because the
effect and the state change are in one transaction guarded by the fencing
token. True exactly-once *execution* isn't possible while workers can crash at
any moment. Exactly-once *effect* is, as long as the effect lives in the same
database.

## Weak spots I already know about

- The guarantee only covers side effects stored in Postgres. An email or an
  S3 upload would be sent again on retry unless it had its own idempotency key.
- Lease length is a trade-off. If it's too short, slow jobs get reclaimed
  while still running, which wastes work (still correct thanks to the token).
  If it's too long, a crashed worker's job waits that long before anyone
  picks it up.
- There is one Postgres instance with no failover, and every claim goes to
  the same table. That is the likely bottleneck under heavy load.
- Workers poll every ~100 ms when idle. That's fine for 8 workers but
  wasteful at hundreds; `LISTEN/NOTIFY` would fix it.
