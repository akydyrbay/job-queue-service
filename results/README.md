# Harness results

Raw output of every harness run, kept as recorded: the original run, the
flawed ones, and the deliberately broken ones. What the harness checks is in
[docs/harness.md](../docs/harness.md).

**Environment for all runs:** macOS, Postgres 16.2 (`pgserver`, local), API
started by the harness (`--start-api`, uvicorn over a unix socket), Python
3.12. Docker was not used for these runs.

## Full runs (10,000 jobs, 8 workers, kill -9 about every 2 s)

| # | Run | Result | Kills | Reclaimed | Cancels sent / accepted | Cancel hit a running job* | Duration | Harness version |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | `20261111-193041` | PASS | 43 | 22 | 808 / 354 | 288 | 94 s | 1 cancel task |
| 2 | `20261111-193241` | PASS | 45 | 12 | 1000 / 510 | 380 | 100 s | 1 cancel task |
| 3 | `20261111-193432` | PASS | 42 | 28 | 143 / 29 | 16 | 92 s | 1 cancel task |
| 4 | `20261111-193634` | PASS | 51 | 11 | 1001 / 36 | 4 | 119 s | 12 cancel tasks |
| 5 | `20261111-193846` | PASS | 52 | 8 | 1004 / 34 | 3 | 119 s | 12 cancel tasks |
| 6 | `20261111-194128` | PASS | 91 | 46 | 1003 / 288 | 98 | 198 s | 12 cancel tasks, 5% slow jobs |
| 7 | `20261111-194459` | PASS | 91 | 43 | 1003 / 279 | 103 | 196 s | 12 cancel tasks, 5% slow jobs |
| 8 | `20261111-195132` | PASS | 46 | 6 | 1005 / 43 | 8 | 98 s | 12 cancel tasks, invariant 8 |

\* Worker `stale_rejected` events: the worker finished a job but its guarded
update was refused, almost always because the job had been cancelled while
it ran.

**8 runs, 8 passed.** That's 80,000 jobs, 461 `kill -9`s and 176 jobs
recovered after lease expiry, with no duplicate side effects and no stuck
jobs.

### Run history: what changed between runs and why

- **Runs 1–3: one cancel task.** It competed with 64 concurrent
  submissions for the API's 10 database connections, and managed about one
  cancel per second while jobs were being submitted. In runs 1 and 2 a
  backlog was left after submission, and most cancels went out then, when
  requests were fast, so they caught many running jobs (288 and 380). In
  run 3 the workers kept up, the queue drained about a second after
  submission ended, and only 143 cancels were sent. **Run 3 passed but
  barely tested cancellation.**
- **Runs 4–5: 12 cancel tasks.** All 1,000 cancels were sent, but almost all
  of them during submission, when every request was slow. Jobs take 20–100
  ms, so a cancel aimed at a running job nearly always arrived after it
  finished: about 35 accepted cancels, and only 3–4 that caught a running
  job. More cancel traffic didn't mean better coverage.
- **Runs 6–7: 5% of jobs take 1.5 s.** Cancels aimed at running jobs now catch
  them mid-run: about 280 accepted cancels, about 140 jobs cancelled after a
  worker had claimed them, and about 100 `stale_rejected` per run. More jobs
  were reclaimed (~45), because a kill is more likely to hit a slow job.
- **Run 8: invariant 8 added** after mutation m3 (below). Slow jobs were
  switched off again for this run.
- Runs 1–7 happened before invariant 8 existed. For runs 1–5 it can be
  checked from the saved stats: no job had more than 2 attempts, so extra
  claims = reclaimed, which is below the kill count in every run. In runs 6
  and 7 a few jobs reached 3 attempts (a slow job's worker was killed twice
  in a row), so the exact number of extra claims wasn't saved, and the
  database has been cleared since. Invariant 8 is unverified for those two
  runs.

### Things noticed in the raw output

- **Run 1:** workers logged 9,193 `succeeded` events, but the database has
  9,194 succeeded jobs. A worker was killed after its commit but before
  printing the log line. This is why the invariants use the database, not
  the logs.
- **Run 1:** one POST failed with `RemoteProtocolError` (the server closed
  the connection). The harness retried with the same key and got the
  original job back.
- **All runs:** some kills hit a worker that was holding a job, but fewer of
  those jobs were reclaimed. A held job could also be cancelled while its
  lease was running out, or finish between the harness's check and the kill.
- Submitting the 10,000 unique jobs took 58–134 s, about 75–170 jobs/s, on
  top of the duplicate and cancel traffic. **Correction (Phase 7):** I first
  read this as the API's limit. It's mostly the harness's own: httpx costs
  about 4.5 ms of client CPU per request, which caps one client process at
  roughly 200 req/s. Measured with a lighter client, the same API handles
  about 400 req/s cleanly (see the load tests below).

## Mutation runs

To check that the harness can fail, I broke the worker in a copy of the repo
and ran the harness against it (2,000 jobs to save time). Raw output is in
`mutations/`.

| Run | Bug introduced | Result | Caught by |
| --- | --- | --- | --- |
| `194911` | **m1:** finish query checks only `WHERE id = ...` (no ownership check) | FAIL ✅ | Invariant 6: 2 jobs whose cancel returned 200 ended `succeeded`. |
| `194939` | **m2:** side effect inserted before the guarded update, outside the transaction | FAIL ✅ | Invariant 3: 4 cancelled or failed jobs have a side effect. |
| `195010` | **m3:** claim as `SELECT` then `UPDATE`, without the row lock | **PASS ❌** | Nothing. There were 322 extra claims, but the guarded finish rejected every losing worker, so invariants 1–7 held. |
| `195105` | m3 again, with invariant 8 added | FAIL ✅ | Invariant 8: 322 extra claims against 9 kills. |

m1 also shows a limit of the harness: removing the ownership check should
allow zombie writes, but `kill -9` never creates a zombie.

**Labelling correction:** the shell loop that started the three mutation runs
used a bash feature that the macOS bash doesn't support, so all three were
labelled "m3". Run 8's note also wrongly said "5% slow jobs". I corrected the
`note` fields in `runs.csv` and `summary.json` (marked `note_corrected`) based
on run order and outcome. No measured values were changed.

## Load tests

Separate from the chaos runs: a fixed request rate with workers running, no
kills. Each `load-*` folder has `load.log`, `api.log` and `summary.json`. The
analysis is in [docs/load_test.md](../docs/load_test.md).

| Run | What | Result |
| --- | --- | --- |
| `load-20261111-202937` | 100 req/s for 5 min, httpx client | **Invalid**: the load generator saturated (95% CPU) while the API sat idle. Stopped by hand, no summary. |
| `load-20261111-204250` | 100 req/s for 20 s, aiohttp (smoke) | PASS |
| `load-20261111-204332` | 100 req/s for 5 min | **PASS**: 0 errors, p99 about 185 ms, queue flat |
| `load-20261111-204859` | Ramp 100 → 1000 req/s | FAIL: backlog grows from 250 req/s, API collapses at 600 |
| `load-20261111-205332` | 600 req/s, API pool 30 | FAIL: CPU-bound, not pool-bound |
| `load-20261111-205438` | 100 req/s, docker-compose workers (4 × 50–300 ms) | FAIL: API fine, backlog grows about 24 jobs/s |

## Long job vs lease

`long-job-20261111-210306.log`: one 3 s job, a 2 s lease, no heartbeat, 2
workers. It ran 5 times (twice on two workers at once) and ended `failed`
although no worker died, with 0 side effects. It's the basis of the verdict in
[REPORT.md](../REPORT.md).
