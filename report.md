# Report: will it survive production?

A training-job queue on FastAPI and PostgreSQL. Workers claim jobs with
`FOR UPDATE SKIP LOCKED` and a lease, and the side effect commits in the same
transaction as the status change, guarded by a fencing token.

## Verdict at 100 requests per second: **DON'T SHIP**

The API is not the problem at 100 req/s. It passed 5 minutes at 100 req/s
with 0 errors, a p99 of about 185 ms, and about 4x CPU headroom
([load test](docs/load_test.md)). The queue's correctness held through 8 chaos
runs and 18 targeted silent-failure tests. Two other findings decide it:

1. **Any job longer than the lease is never finished.** The lease is 10 s and
   there is no heartbeat, and training jobs run for minutes. I ran a 3 s job
   against a 2 s lease with two workers
   ([`results/long-job-20261111-210306.log`](results/long-job-20261111-210306.log)).
   The job ran **5 times**, twice on two workers at once, and ended `failed`
   with "lease expired on every one of 5 attempts", although no worker ever
   died. The exactly-once *effect* held (0 duplicate side effects), but the
   job never succeeds.
2. **The shipped worker configuration falls behind at 100 req/s, and nothing
   says so.** With the docker-compose settings (4 workers), the API stayed
   perfect (0 errors, p99 94 ms) while the backlog grew by about 24 jobs per
   second: 1,472 waiting after 60 s. No endpoint, metric or log line reports
   a growing queue.

**What to fix first: lease renewal.** While it works on a job, the worker
should extend its lease every few seconds. The update should be guarded by
`worker_id` and `attempts` like the finish, and the worker should stop if the
extension matches 0 rows, because that means it lost the job. This is the only
finding that breaks *every* real job, at any request rate. Then, in order:
- expose queue depth and the age of the oldest queued job, and alert on
  either rising;
- size the workers as arrival rate × job duration;
- start the API pool at its full size, to remove the cold-start latency spike.

## Deliverables

| | |
| --- | --- |
| Design note (written before the API and worker code) | [docs/design_note.md](docs/design_note.md), schema details in [docs/design.md](docs/design.md) |
| Chaos harness | [harness/harness.py](harness/harness.py), explained in [docs/harness.md](docs/harness.md), including **what it doesn't catch** |
| Raw output of every run, failed ones included | [results/](results/README.md): 8 full chaos runs (all passed), 4 runs against deliberately broken workers, 6 load runs (1 invalid, 3 failed), the long-job run, and the silent-failure test log |
| Silent failures, and how each was checked | [docs/silent_failures.md](docs/silent_failures.md) and [tests/test_silent_failures.py](tests/test_silent_failures.py) |
| Load test | [docs/load_test.md](docs/load_test.md) |

**Number of chaos runs:** 8 full runs (10,000 jobs, 8 workers, `kill -9`
about every 2 s), 8 passed: 80,000 jobs, 461 kills, 176 jobs recovered after
lease expiry, 0 duplicate side effects, 0 stuck jobs. There were also 4
mutation runs at 2,000 jobs; one of them passed when it should have failed,
which is why invariant 8 exists.

## What surprised me, and what still concerns me

What surprised me most was how often a check passed without testing
anything. The chaos harness passed against a worker whose claim had no row
lock at all, because the guarded finish quietly rejected every double claim,
so the bug only showed up as wasted work. The cancel/complete race test passed
300 out of 300 times, and cancel won every one, because the finish needs four
round trips to the database and the cancel one: the "finish wins" path was
never run. My first 100 req/s load test showed the service collapsing, when
it was really my load generator using a full core while the API sat idle.
Each time, the fix was to check that the test could fail before trusting it
when it passed. What still concerns me is the gap between what I tested and
how a training queue is used. Every job in my tests lasted under 2 s, so the
lease design was never under pressure, and the one experiment with a longer
job showed it failing completely. I also worry that the most likely
production failure, a backlog that grows while every API metric looks
healthy, is invisible to the system as built. And the absolute numbers come
from one laptop shared with other apps, not production hardware.

## If something broke: what happened, what I tried, what I learned

| What broke | What I did | What I learned |
| --- | --- | --- |
| A job longer than the lease ran 5 times and was marked failed | Reproduced with real workers ([log](results/long-job-20261111-210306.log)); left unfixed and made it the first fix | The guarantee (effect at most once) and liveness (the job ever finishes) are separate. My tests only checked the first. |
| Mutation m3 (claim without a row lock) **passed** the harness | Added invariant 8: extra claims ≤ kills. m3 now fails with 322 extra claims against 9 kills | Defence in depth hides bugs from end-state checks. Count the waste, not just the outcome. |
| Mutation m1 (no ownership check) never produced a zombie write | Documented it as a harness blind spot, and added a real `SIGSTOP`/`SIGCONT` zombie test | `kill -9` never brings a worker back, so it can't test stale writes. Freezing a process can. |
| Run 3 sent only 143 cancels; runs 4–5 hit a running job only 3–4 times | 12 parallel cancel tasks, then 5% slow jobs (runs 6–7: about 100 per run). All runs kept as recorded | Coverage depends on timing. Count how often the dangerous interleaving actually happened, not just how many requests were sent. |
| The race test: cancel won 300 of 300 | Random 0–3 ms delay on each side, and the test fails unless both sides win at least once (now about 190 to 110) | A barrier doesn't make a fair race when one side does more round trips. |
| First 100 req/s load run "collapsed" | Measured client CPU: httpx 4.5 ms per request, aiohttp 0.37 ms. Switched to aiohttp, and the same load passed | Record the load generator's own CPU and lag, or you're measuring the tool. This also corrected a Phase 5 claim about API throughput. |
| `test_worker_ids_are_unique` failed about 1 run in 13 | A 2-byte random suffix collides about 7% of the time across 100 ids (birthday problem); widened it to 4 bytes | A flaky test is a bug in the test, even when production is unaffected. |
| Three mutation runs were all labelled "m3"; one load-run note was wrong | Corrected only the `note` fields, marked them `note_corrected`, and explained it in `results/README.md`. No measured values changed | macOS ships bash 3.2, which has no associative arrays. Check labels against the data. |

## AI tool disclosure

### Summary

I built this with **Claude Code** (Claude Opus 5.5). It did most of the
hands-on work: it wrote the code, tests and docs, ran the experiments and
drafted the analysis. I directed the work phase by phase, reviewed and
committed each phase, and made the changes listed below. This note says
which part is which.

### What I did

- **Experiments:** ran every chaos, mutation, silent-failure, load and
  long-job run in `results/`, on my machine against a local Postgres 16
  (`pgserver`), **not Docker**.
- **Analysis:** final drafts of the results docs, the verdict and the
  reflection paragraph.
- **Made decisions along the way.** I merged the dev requirements into one
  file and removed the harness's slow-job option after runs 6–7.

### What AI did 

- **Design:** proposed and drafted the design note and the schema: the
  single-statement claim, the `attempts` fencing token, finishing with the
  side effect in the same guarded transaction, cancel semantics,
  `MAX_ATTEMPTS`, and `READ COMMITTED`.
- **Code:** the API, the worker, the chaos harness, the load test, and all
  tests which i throughly reviewed.

### Core logic: what was proposed and how it was checked

| Component | Design | Checked by |
| --- | --- | --- |
| Claim | One `UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED)` | 16 threads draining 500 jobs, each claimed once; a locked row is skipped without waiting; a naive claim double-claims (negative control) |
| Finish / fencing | Requires `running`, the same `worker_id` and `attempts`, and an unexpired lease | A real `SIGSTOP`/`SIGCONT` zombie is rejected; an unguarded finish lets it overwrite (negative control) |
| Effect + status | Side-effect row and status change in one transaction | A connection lost before COMMIT leaves no trace; 8 chaos runs with 0 duplicate effects |
| Cancel | Only from `queued`/`running`; the row lock decides the race | 300 forced races where both sides win and the state always matches the effects |
| Idempotent create | `UNIQUE` key + `ON CONFLICT`, then read back | A retry against an uncommitted insert waits and returns the same job; 100 concurrent POSTs create 1 job |

### Where the AI was wrong or unreliable

All of these were found during the work and kept in the record:
- It **blamed the API for its own load generator's limit**. httpx used 4.5 ms
  of CPU per request; the API was idle.
- It **wrote a race test that never exercised one side**: cancel won 300 out
  of 300.
- Its **first harness passed a broken claim** (mutation m3), which led to
  invariant 8.
- It **wrote a flaky test**: worker id collisions about 7% of the time.
- It **mislabelled three runs** because of a bash incompatibility, and its
  first summary of the Phase 5 runs had **wrong figures**. Both were corrected
  against the saved data.
- It did **not test jobs longer than the lease** until the end, although
  that case breaks every real training job.

### How we know the checks can fail

- **Mutation runs.** Three deliberately broken workers: m1 and m2 were
  caught, m3 was missed until invariant 8 was added.
- **Negative controls.** Each silent-failure test is paired with the same
  scenario without the protection, which does go wrong.
- **Failed runs are kept.** The invalid httpx load run, the failed capacity
  runs, and the runs with weak cancel coverage are all in `results/`.

### Limits

No result was produced with Docker. The load numbers come from one laptop
shared with other apps. The verdict is about this simulated workload and
configuration.

## How to run

```bash
docker compose up --build                 # API on :8000, 4 workers, Postgres 16
python -m harness.harness --reset --api-url http://localhost:8000 \
  --database-url postgresql://jobqueue:jobqueue@localhost:5432/jobqueue   # stop the compose workers first
python -m harness.load --reset --start-api \
  --database-url postgresql://jobqueue:jobqueue@localhost:5432/jobqueue --stages 100:300
TEST_DATABASE_URL=postgresql://jobqueue:jobqueue@localhost:5432/jobqueue_test pytest -q
```
