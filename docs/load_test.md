# Load test: can it take 100 requests per second?

`harness/load.py` sends a fixed rate of API traffic while workers process the
jobs, and records latency, errors, queue depth, database connections, and CPU
for both the load generator and the API. Raw output of every run is in
`results/load-*/` (`load.log`, `api.log`, `summary.json`).

## How it measures

- **Open loop.** Requests go out on a fixed schedule, whether or not earlier
  ones have returned. Latency is measured from when a request *should* have
  started. A closed loop ("next request when the last one returns") slows
  down with the server and hides its latency.
- **Traffic mix:** 50% create, 30% get, 10% list (random page 1–10), 10%
  cancel. So at 100 req/s, about 50 new jobs arrive per second.
- **Workers:** 8 processes, with 20–100 ms of simulated work and 5% failures,
  unless noted.
- **Pass criteria, fixed before the first run:** errors below 0.1%, p99 below
  500 ms for every operation, and a queue backlog that doesn't keep growing.
- **Generator lag and CPU are recorded**, so an overloaded load generator
  can't be mistaken for a slow server. This turned out to matter (see the
  first run below).

**Environment:** a 4-core MacBook running Postgres 16.2, one uvicorn process
(as in the Dockerfile), the workers and the load generator all at once, with
the API on a unix socket. During testing a browser tab and the window server
were using about 1.3 cores. These are not production numbers. They show
where the limits are and how far apart they are.

## Runs

| Run | What | Result |
| --- | --- | --- |
| `load-20261111-202937` | 100 req/s for 5 min, **httpx client** | **Invalid.** The load generator collapsed, not the server (see below). Stopped by hand; no summary. |
| `load-20261111-204250` | 100 req/s for 20 s, aiohttp client (smoke test) | PASS |
| `load-20261111-204332` | **100 req/s for 5 min** | **PASS** |
| `load-20261111-204859` | Ramp 100 → 250 → 400 → 600 → 1000 req/s, 30 s each | FAIL from 250 (backlog), collapse at 600 |
| `load-20261111-205332` | 600 req/s, API pool raised from 10 to 30 | FAIL: still collapses, so it's CPU-bound |
| `load-20261111-205438` | 100 req/s for 60 s with the **docker-compose worker settings** (4 workers, 50–300 ms jobs) | FAIL: the API is fine but the backlog grows |

Two earlier smoke runs (10 s at 50 req/s, with httpx) were not kept. They
showed a p99 of 2.3 s in the first seconds, which led me to add a 5 s settle
period after starting the workers. Starting 8 Python processes saturates the
CPU for a few seconds.

### The first 100 req/s run measured my tool, not the service

At 100 req/s the system appeared to collapse within 15 s. Throughput fell to
20 req/s, p50 latency rose to 27 s, and progress reports stopped arriving on
time. While it was stuck:
- the **load generator was at 95% CPU** (one full core);
- **the API was at 0% CPU**, idle, waiting for requests that never came.

A micro-benchmark against a real API process showed **httpx costs about 4.5 ms
of client CPU per request**, which caps one client process at roughly 200
req/s. aiohttp costs **0.37 ms**. In open-loop mode the problem feeds itself:
slow responses mean more open connections, each request then costs more client
CPU, and the generator falls further behind. After switching to aiohttp, the
same 100 req/s used 15% of a core on the client and 25% on the API.

This also changed a Phase 5 conclusion. The chaos harness uses httpx, so the
75–170 jobs/s it achieved was mostly the harness's own limit, not the API's.
I corrected that in [results/README.md](../results/README.md).

## Results

### 100 req/s for 5 minutes: PASS

| | create | get | list | cancel |
| --- | --- | --- | --- | --- |
| requests | 15,074 | 8,930 | 2,976 | 3,020 |
| p50 | 5.4 ms | 4.2 ms | 7.9 ms | 4.7 ms |
| p95 | 19.8 ms | 17.8 ms | 22.3 ms | 15.2 ms |
| p99 | 184 ms | 186 ms | 185 ms | 162 ms |
| max | 1.07 s | 1.08 s | 1.18 s | 0.98 s |

- 30,000 requests with **0 errors**.
- **The queue stayed flat**: at most 10 jobs waiting, and it was empty 1 s
  after the load stopped.
- The API used about 24% of one core on average. The database had at most 20
  connections: 8 workers, the API's pool of 10, and the load test's sampler.
- **The p99 is made of short spikes, not a steady tail.** Most 5 s windows
  have a p99 of 10–14 ms; a few have 200–275 ms. Generator lag spikes at the
  same moments (up to 535 ms), so the client and the server stall together.
  That points at the machine (other apps, scheduling), not the service, but I
  can't prove it from these runs.
- **Cold start:** in the ramp run, the 30 s stage at 100 req/s *failed*
  (p99 about 600 ms). Almost all of that came from its first 5 s window,
  right after start-up; the rest of the stage looked like the 5-minute run.
  The API's pool starts with one connection (`DB_POOL_MIN_SIZE=1`) and opens
  more as needed. Setting the minimum to the maximum would likely remove this
  spike, but I haven't tested that.

### Ramp: what breaks first

| Target | Errors | p50 (create) | p99 (create) | Queue (start → end of stage) | API CPU | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| 100 req/s | 0% | 5.5 ms | 582 ms (cold start) | 2 → 1, flat | 25% | FAIL (cold start) |
| 250 req/s | 0% | 3.6 ms | 127 ms | **25 → 206, growing** | 45% | FAIL (backlog) |
| 400 req/s | 0% | 3.6 ms | 372 ms | **599 → 2,580, growing** | 64% | FAIL (backlog) |
| 600 req/s | **21%** | **3.5 s** | 13.2 s | 3,100 → 5,600 | 71–89% | FAIL (collapse) |
| 1000 req/s | **84%** | 10.1 s | 14.4 s | 5,800 → 6,800 | 59% | FAIL; generator lag p50 316 ms, so partly invalid |

**1. The workers break first, and silently.** At 250 req/s (about 125 new jobs
per second), the API looks perfect: 0 errors and a p99 of about 120 ms. Yet
the backlog grows by about 6 jobs every second, because 8 workers with
20–100 ms jobs complete roughly 90–110 jobs/s. After the ramp, the 7,200 job
backlog drained at about 87 jobs/s. **Nothing in the system reports this.** No
endpoint and no log line says "the queue is growing".

**2. Then the API process runs out of CPU** at about 450–500 req/s. One
uvicorn process is one Python event loop on one core. At 400 req/s it still
had 0 errors, with p95 rising to about 250 ms. At 600 req/s requests queue up,
time out after 10 s, and the server stops accepting connections fast enough
(`UnixClientConnectorError`).

**3. The connection pool is not the limit.** I reran 600 req/s with the API
pool raised from 10 to 30 connections. It still topped out at about 520 req/s,
with the API at 85–93% CPU.

**4. Not reached in these tests: Postgres connections.** To go past ~500
req/s you'd run more API processes. Each has its own pool, and Postgres here
allows 100 connections. About 8 API processes with 10 connections each, plus
the workers, would hit that. Beyond that you need a connection pooler such as
PgBouncer.

### The docker-compose settings at 100 req/s

`docker compose up` ships 4 workers with 50–300 ms jobs. At 100 req/s
(about 50 new jobs/s):

- **The API is fine:** 0 errors, p50 5.6 ms, p99 94 ms (create).
- **The queue is not:** it grew to 1,472 waiting jobs in 60 s, because 4
  workers only complete about 21–26 jobs/s. It took 70 s to drain once the
  load stopped.

So whether the system "survives 100 req/s" depends on job duration and
worker count. The code doesn't decide it; capacity planning does. Required
workers ≈ jobs per second × average job duration. 50 jobs/s of 175 ms jobs
needs at least 9 workers; real training jobs that take minutes would need
hundreds.

## What these tests don't show

- **Production hardware.** Everything shared one laptop with other apps
  running. Absolute numbers will differ; the order of the limits (workers,
  then API CPU, then database connections) is what carries over.
- **Docker and TCP.** These runs used a unix socket and a local Postgres, not
  the compose network.
- **Large tables.** The jobs table grew to about 35,000 rows. `GET /jobs` runs
  a `COUNT(*)` on every call, which gets slower as the table grows. With
  millions of rows that would become a problem. Untested.
- **Long runs.** The longest steady run was 5 minutes. Slow leaks
  (connections, memory, table bloat from constant updates) wouldn't show up.
- **Failures under load.** Workers weren't killed during these runs; that's
  the chaos harness's job, at a lower rate.
- **Real job durations.** Jobs are simulated with sleeps of 20–300 ms.

## What the numbers say about the verdict

For the API and the queue logic, 100 req/s is comfortable. It passed 5
minutes with 0 errors, a p99 of about 185 ms and about 4x CPU headroom, on top
of the correctness evidence from the chaos harness and silent-failure tests.

Two findings work against shipping as-is:
- **The shipped configuration doesn't keep up.** With the docker-compose
  workers, the backlog grows at 100 req/s.
- **Nothing alerts on it.** The first symptom would be users noticing jobs
  that never start.

**The first thing to fix is visibility, not throughput:** expose queue depth
and the age of the oldest queued job, and alert when either keeps rising.
Then size the workers from job duration and arrival rate. Setting the API
pool's minimum size to its maximum is a cheap fix for the cold-start tail.
