# Silent failures

These are bugs that don't raise an error. The system keeps running and the
data is quietly wrong. For each one the task asks: what protects against it,
and how did I check?

Every check is a test in `tests/test_silent_failures.py`. Wherever it could be
done deterministically, I also wrote a **negative control**: the same scenario
without the protection, showing that it really goes wrong. A test that has
never seen the bug can't tell you it would catch it. Raw output:
[`results/silent-failures-20261111-201122.log`](../results/silent-failures-20261111-201122.log)
(18 passed; the file was also run 3 more times with 18 passed each time).

| Failure | Protection | Checked by | Result |
| --- | --- | --- | --- |
| Double claim | One `UPDATE ... (SELECT ... FOR UPDATE SKIP LOCKED)` | 3 tests + harness invariant 8 + mutation m3 | ✅ |
| Zombie worker | Finish guarded by `worker_id`, `attempts`, lease | Real frozen process (SIGSTOP) + negative control + mutation m1 | ✅ |
| Cancel/complete race | Finish requires `status = 'running'`; one row lock decides | 300 forced races + negative control + harness invariant 6 | ✅ |
| Duplicate on retry | `UNIQUE (idempotency_key)` + `ON CONFLICT DO NOTHING` | Retry against an uncommitted insert, 100 concurrent POSTs, harness | ✅ |
| Isolation level | `READ COMMITTED` + row locks | Verified setting, `REPEATABLE READ` comparison, negative control | ✅ |
| Clock skew | Only the database's `NOW()` | Source scan, real worker with clock ±1 h, negative control | ✅ |

---

## 1. Double claim

**What goes wrong:** two workers both take the same job and both run it. With
my guarded finish the second write is rejected, so nothing ends up duplicated
in the data. That is exactly why it's silent: the only symptom is wasted work.

**Protection:** the claim is a single statement. The inner
`SELECT ... FOR UPDATE SKIP LOCKED` locks the row it picks, and other workers
skip locked rows instead of reading them.

**How I checked:**
- `test_double_claim_locked_row_is_skipped_not_shared`: worker A claims j1
  and keeps its transaction open. Worker B, with a 1 s statement timeout,
  claims and gets j2 straight away. It neither waits on A's lock nor gets j1.
- `test_double_claim_16_threads_each_job_claimed_once`: 16 threads start
  together and drain 500 jobs. That's 500 claims, 500 distinct ids, and no
  job with more than 1 attempt.
- **Negative control** `test_negative_control_naive_claim_double_claims`: with
  a SELECT and then an UPDATE (no lock), both workers see the same free job
  and both take it (`attempts = 2`).
- **Harness:** mutation m3 (naive claim) produced 322 extra claims in a 2,000
  job run, and **invariants 1–7 still passed**. I added invariant 8 (extra
  claims ≤ kills) because of this. See [harness.md](harness.md).

## 2. Zombie worker

**What goes wrong:** a worker stalls (GC pause, suspended VM, network hiccup),
its lease expires, and another worker takes over. Then the first one wakes
up and writes its old result over the new one, or adds a second side effect.

**Protection:** the finish only applies if the job is still `running` with
*my* `worker_id`, *my* `attempts` value and an unexpired lease. Otherwise it
matches 0 rows and the transaction (including the side effect) rolls back.

**How I checked:**
- `test_frozen_worker_process_cannot_write_after_lease_expired` uses **real
  worker processes**. Worker A claims a 1-second job and is frozen with
  `SIGSTOP`. Its 2 s lease expires, worker B claims the job (attempt 2) and
  finishes it. Then A is resumed with `SIGCONT`, finishes its "work" and tries
  to write. Its log shows `event=stale_rejected ... attempt=1`. The job ends
  `succeeded` by B, with exactly one side effect (attempt 2).
- Two tests in `tests/test_worker.py` cover the same case step by step: a
  zombie after another worker reclaimed the job, and a late finish when
  nobody reclaimed it yet (the lease check alone rejects it).
- **Negative control** `test_negative_control_unguarded_finish_lets_zombie_overwrite`:
  with an unguarded finish, A's stale result replaces B's and the job has two
  side effects.
- The chaos harness **can't** test this: a worker killed with `kill -9` never
  comes back.

**"A worker froze for 5 minutes and came back."** The length of the freeze
doesn't matter once it's longer than the lease (10 s). After that the job is
reclaimed, and the frozen worker's write is rejected when it wakes up. If
nobody had reclaimed the job yet, the write is still rejected because the
lease has expired, and the job is picked up on the next claim. The work done
by the frozen worker is wasted, but nothing is wrong in the data.

## 3. Cancel / complete race

**What goes wrong:** the API answers "cancelled", but the worker finishes at
the same moment and the job ends `succeeded`, or ends `cancelled` with a side
effect anyway.

**Protection:** cancel only changes `queued`/`running` jobs; finish requires
`status = 'running'`. Both are single-row updates, so Postgres's row lock
orders them. Whichever comes second re-checks its condition against the
first one's result and matches 0 rows.

**How I checked:**
- `test_cancel_and_complete_racing_never_mix`: 300 times, the finish and the
  cancel run in two threads released together. Each time exactly one wins,
  and the state matches: `cancelled` with 0 effects, or `succeeded` with 1.
  The recorded run had cancel winning 209 times and finish 91 times.
- **Lesson from writing this test:** my first version released both threads at
  exactly the same moment, and **cancel won all 300 times**. Finish needs four
  round trips (BEGIN, UPDATE, INSERT, COMMIT) and cancel one, so cancel always
  got the lock first. The test passed, but the "finish wins" path was never
  exercised. Now each side waits a random 0–3 ms, and the test fails unless
  both sides win at least once.
- **Negative control** `test_negative_control_finish_without_status_check_overrides_cancel`:
  if finish only checked ownership, a cancel that already returned 200 is
  silently turned into `succeeded`.
- **Harness:** invariant 6 in every run, with up to 380 cancels per run
  landing on a running job. Mutation m1 (no guard) was caught by invariant 6.

## 4. Duplicate on request retry

**What goes wrong:** the client times out and retries, but the first request
actually succeeded, so there are now two jobs for one intent.

**Protection:** `UNIQUE (idempotency_key)` plus `INSERT ... ON CONFLICT DO
NOTHING`, then reading the existing row back. The API never checks "does it
exist?" before inserting.

**How I checked:**
- `test_retry_while_first_insert_is_uncommitted_returns_the_same_job`: the
  hardest timing. The first request's INSERT is still uncommitted when the
  retry arrives. The retry **waits** (still pending after 0.5 s), and once the
  first commits it returns that job (200, same id). There's 1 row.
- `test_retry_after_first_insert_rolled_back_creates_the_job`: if the first
  insert rolls back instead, the retry creates the job (201).
- `test_100_concurrent_posts_with_one_key`: one 201, ninety-nine 200s, one id.
- **Negative control** in section 5: check-then-insert creates duplicates.
- **Harness:** invariant 5 in every run. About 1,000 jobs per run were POSTed
  twice simultaneously, plus around 550 later duplicates. One real dropped
  connection in run 1 was retried and returned the original job.

## 5. Transaction isolation level

**What goes wrong:** a correct-looking pattern behaves differently at a
different isolation level. Either duplicates appear, or workers start failing
and need retry logic.

**Protection:** everything runs at Postgres's default, `READ COMMITTED`.
Correctness comes from the row locks and the unique constraint, not from
snapshot isolation.

**How I checked:**
- `test_api_and_worker_connections_use_read_committed`: both the API pool and
  the worker connection really run at `read committed`.
- `test_claim_under_repeatable_read_fails_instead_of_skipping`: under
  `REPEATABLE READ`, a worker whose snapshot is older than another worker's
  claim gets `SerializationFailure` instead of moving on to the next job.
  Under load most claims would hit this and need retries. The same sequence
  under `READ COMMITTED` just returns the next job.
- **Negative control** `test_negative_control_check_then_insert_duplicates_under_read_committed`:
  without a unique constraint, two requests both see "no row" and both
  insert: 2 rows, silently. Against the real `jobs` table, the same pattern
  fails loudly with `UniqueViolation`.

## 6. Clock skew

**What goes wrong:** a worker whose clock is behind gives itself a lease that
has already expired, so another worker reclaims a job that is still running.
A worker whose clock is ahead gives itself a lease that lasts too long, so its
job sits there after a crash.

**Protection:** every lease is set and compared with the database's `NOW()`.
Workers never read the wall clock. `time.monotonic()` is only used to space
out the give-up check and is never compared with the database.

**How I checked:**
- `test_no_wall_clock_reads_in_service_code`: scans `app/` and `worker/` for
  `time.time()`, `datetime.now()`, `utcnow()` and `date.today()`. None found.
- `test_worker_with_skewed_clock_gets_a_normal_lease[+1h, -1h]`: a **real
  worker process** with `time.time()` shifted by an hour (via
  `sitecustomize`; a marker file proves the shift was active). Its lease is
  still within `NOW() + LEASE_SECONDS` on the database clock, and the job
  completes with one side effect.
- **Negative control** `test_negative_control_lease_from_worker_clock_is_reclaimed_immediately`:
  a lease computed from a clock 1 hour slow is already in the past, and
  another worker takes the job at once.
- **Limit:** the shift only affects Python's `time.time()`, not the OS clock.
  Since the service never reads either, the source scan is the stronger
  evidence. A jump in the **database's** clock is not covered; it would move
  every lease at once.

---

## Also checked

- **Worker dies in the middle of its finishing transaction.**
  `test_crash_between_status_update_and_commit_leaves_no_trace`: the status
  update and the side effect have both run, then the connection disappears
  without COMMIT. Postgres rolls back both; the job is still `running` with
  no side effect, and it will be reclaimed when the lease expires.
- **Worker commits and then dies before logging it.** Nothing to protect: the
  commit is the outcome. Seen for real in harness run 1, where the database had
  one more `succeeded` job than the worker logs.

## Not covered (yet)

- **Connection-pool exhaustion in the API.** With 64 concurrent requests and
  10 connections, requests waited but none failed in the harness. If the wait
  exceeds the pool timeout (30 s) the request fails with a 500, which is loud,
  not silent. Load behaviour is Phase 7.
- **Jobs longer than the lease.** Not silent in the data, because the guarded
  finish rejects the duplicate, but they waste work, and after 5 attempts the
  job is marked `failed` although nothing crashed. A heartbeat that extends
  the lease would fix it. Noted in the design note.
- **Database clock jumps, Postgres failover, network partitions.** Not
  simulated.
