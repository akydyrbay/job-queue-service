# Chaos harness

`harness/harness.py` checks whether the queue keeps its promises while things
go wrong. Run results are in [`results/`](../results/README.md).

## Running it

```bash
docker compose up -d db api          # not the worker service: the harness runs its own workers
python -m harness.harness --reset \
  --api-url http://localhost:8000 \
  --database-url postgresql://jobqueue:jobqueue@localhost:5432/jobqueue
```

Without Docker, `--start-api` starts uvicorn on a unix socket for the run.
`--reset` empties the jobs table first; without it the harness refuses to
start if the table already has rows. Exit code 0 means every invariant passed.

Each run writes `results/<timestamp>/`:
- `harness.log`: everything printed during the run;
- `summary.json`: arguments, invariant results, stats, and every kill;
- `workers.log.gz`: all worker logs.

It also adds one line to `results/runs.csv`.

## What it does to the system

All of these happen at the same time:

- **8 worker processes.** About every 2 seconds (±25%) one is chosen at random
  and killed with `kill -9`, and a replacement is started. The harness logs
  which job the killed worker was holding.
- **10,000 unique jobs**, POSTed with 64 requests in flight.
- **Racing duplicates:** 10% of jobs are POSTed twice at the same moment.
- **Late duplicates:** 4 tasks keep re-POSTing keys that were already
  accepted.
- **Cancels:** 12 tasks send 1,000 cancels in total. Half go to a random job
  and half to a job the API currently lists as `running`.
- Failed requests are retried with the same idempotency key, which is what a
  real client would do.

The workers run with a 5 s lease, 20–100 ms of simulated work per job, and a
5% chance that a job fails.

## Invariants

Each is checked against the database, not the worker logs. The logs can be
wrong: a worker killed after its commit but before printing never logs the
job it finished (seen in run 1).

| # | Invariant | How |
| --- | --- | --- |
| 1 | No job lost | 10,000 rows. Every key exists, and every id the API returned is the id stored for that key. |
| 2 | At most one side effect | No `job_id` appears more than once in `job_execution_logs`. |
| 3 | Effect matches state | Every `succeeded` job has exactly 1 effect; every `failed` and `cancelled` job has 0. The effect row's worker and attempt match the job's final worker and attempt. |
| 4 | Nothing stuck | No job is left `queued` or `running`, and the queue drained before the timeout. A timeout counts as a failure. |
| 5 | Consistent final state | Every POST for a key returned the same id, at most one returned 201, and none returned 409. `GET /jobs/{id}` matches the database for 500 random jobs plus every job a cancel was sent for. `GET /jobs` totals per status match the database. |
| 6 | Cancels respected | Every cancel that returned 200 ended `cancelled`. Every cancel that returned 409 hit a `succeeded` or `failed` job. No job is `cancelled` unless a cancel was sent for it. |
| 7 | Only harness workers ran jobs | Every `worker_id` belongs to a process the harness started. A stray `docker compose` worker would make the kill test meaningless. |
| 8 | No double claims | Total extra claims (`attempts - 1`, summed over all jobs) is at most the number of kills. |

### Why invariant 8 exists

I tested the harness by breaking the worker on purpose
([mutation runs](../results/README.md#mutation-runs)). When the claim lost its
row lock, two workers often claimed the same job, but invariants 1–7 still
passed. The guarded finish rejects whichever worker loses, so no duplicate
effect is ever written. The bug only showed up as wasted work. Each kill can
orphan at most one job, so more extra claims than kills means jobs were
claimed twice.

## What the harness does NOT catch

- **Zombie workers.** `kill -9` never brings a worker back, so a stale write
  after lease expiry never happens here. With the ownership check removed
  from the finish query (mutation m1), the harness caught the broken cancels
  but saw no zombie writes. Zombies are covered by the step-by-step tests in
  `tests/test_worker.py`.
- **Jobs longer than the lease.** All jobs finish well inside the 5 s lease.
  A job that outlives its lease is reclaimed and runs again (wasted work), and
  after 5 attempts it is marked `failed` even though no worker died. The
  harness never exercises this.
- **Database failures.** Postgres crashing or failing over, network
  partitions between workers and the database, and dropped connections in
  the middle of a transaction are not simulated. Only worker processes are
  killed.
- **The API process dying.** Only workers are killed. A killed API between
  INSERT and response would show up as a client retry, which the harness
  does exercise (one `RemoteProtocolError` was retried in run 1), but not
  under deliberate API crashes.
- **Side effects outside Postgres.** The side effect is a row in the same
  database, which is why exactly-once effect holds. An email or file upload
  would not be protected.
- **Clock problems.** Leases use the database clock only. The harness never
  skews worker clocks or jumps the database clock.
- **Load.** 10,000 jobs at about 75–170 jobs/s (plus duplicates and
  cancels) through one uvicorn process. Behaviour at 10x is untested
- **Cancel/finish races, reliably.** How many cancels land on a running job
  depends on timing. It ranged from 3 to 380 per run (see
  [results](../results/README.md)). With jobs of 20–100 ms, cancels sent
  under load usually arrive after the job finished. The exact race (cancel
  and finish at the same instant) is better covered by a deterministic test
- **Rare interleavings.** Eight runs of 10,000 jobs cover common orderings.
  Something that needs a precise timing (e.g. a kill inside the half
  millisecond between two statements) could still be missed. The
  deterministic tests in `tests/` cover the orderings I could think of.
- **Different isolation levels or Postgres settings.** Every run used the
  defaults (`READ COMMITTED`).
