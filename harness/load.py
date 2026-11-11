"""Open-loop load test: a fixed request rate for a fixed time, with workers running.

Requests are fired on a schedule, not "next one when the last one returns",
so a slow server can't slow the load down and hide its own latency. Latency
is measured from when each request *should* have started; if the generator
itself falls behind, that delay counts against the server, and the generator
lag is reported separately so it can be told apart.

It uses aiohttp, not httpx: httpx costs ~4.5 ms of client CPU per request
(measured), which caps one generator process at ~200 req/s. Generator and API
CPU are sampled every second so a saturated client can't pass for a slow server.

    python -m harness.load --reset --start-api --database-url ... --stages 100:300
    python -m harness.load --reset --start-api --database-url ... --stages 100:30,250:30,500:30,1000:30

Pass criteria for a stage (fixed before running, see docs/load_test.md):
  - errors (5xx, timeouts, connection failures) < 0.1% of requests
  - p99 latency < 500 ms for every operation
  - the queue backlog does not keep growing
"""
import argparse
import asyncio
import json
import os
import random
import shutil
import sys
import tempfile
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import aiohttp
import psycopg
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parent.parent
OPS = ("create", "get", "list", "cancel")
P99_LIMIT_MS = 500
ERROR_LIMIT = 0.001


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--api-url", default="http://localhost:8000")
    p.add_argument("--start-api", action="store_true", help="start uvicorn on a unix socket for the run")
    p.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    p.add_argument("--reset", action="store_true", help="TRUNCATE jobs before the run")
    p.add_argument("--stages", default="100:300",
                   help="comma-separated rate:seconds, e.g. 100:30,500:30")
    p.add_argument("--mix", default="create=50,get=30,list=10,cancel=10",
                   help="operation weights")
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--work-min-ms", type=int, default=20)
    p.add_argument("--work-max-ms", type=int, default=100)
    p.add_argument("--fail-rate", type=float, default=0.05)
    p.add_argument("--settle", type=float, default=5,
                   help="seconds between starting the workers and starting the load")
    p.add_argument("--timeout", type=float, default=10, help="per-request timeout in seconds")
    p.add_argument("--max-connections", type=int, default=1000, help="HTTP client connection limit")
    p.add_argument("--drain-wait", type=float, default=60,
                   help="after the load stops, seconds to wait for the backlog to clear")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--results-dir", default=str(ROOT / "results"))
    p.add_argument("--note", default="")
    args = p.parse_args(argv)
    if not args.database_url:
        p.error("--database-url or DATABASE_URL is required")
    args.stage_list = [tuple(float(x) for x in s.split(":")) for s in args.stages.split(",")]
    args.mix_weights = {k: float(v) for k, v in (kv.split("=") for kv in args.mix.split(","))}
    return args


def pct(values, q):
    if not values:
        return None
    values = sorted(values)
    return values[min(len(values) - 1, int(q / 100 * len(values)))]


def ms(seconds):
    return None if seconds is None else round(seconds * 1000, 1)


class Log:
    def __init__(self, path: Path):
        self.file = open(path, "w", buffering=1)
        self.t0 = time.monotonic()

    def __call__(self, msg=""):
        line = f"[{time.monotonic() - self.t0:7.1f}s] {msg}"
        print(line, flush=True)
        self.file.write(line + "\n")


class LoadTest:
    def __init__(self, args, run_dir: Path, log: Log):
        self.args = args
        self.run_dir = run_dir
        self.log = log
        self.rng = random.Random(args.seed)
        self.ops, self.weights = zip(*args.mix_weights.items())
        self.job_ids: list[str] = []
        # (stage, op, scheduled_at, latency_s, service_s, outcome)
        self.records: list[tuple] = []
        self.lags: dict[int, list[float]] = defaultdict(list)
        self.samples: list[dict] = []  # once per second: queue depth, db connections
        self.workers: list[asyncio.subprocess.Process] = []
        self.stop_sampling = asyncio.Event()
        self.t0 = 0.0
        self.key_seq = 0
        self.api_pid: int | None = None

    async def start_workers(self):
        log_dir = self.run_dir / "workers"
        log_dir.mkdir(exist_ok=True)
        env = {
            **os.environ,
            "DATABASE_URL": self.args.database_url,
            "WORK_MIN_MS": str(self.args.work_min_ms),
            "WORK_MAX_MS": str(self.args.work_max_ms),
            "FAIL_RATE": str(self.args.fail_rate),
        }
        for n in range(self.args.workers):
            out = open(log_dir / f"worker-{n:02d}.log", "w")
            self.workers.append(await asyncio.create_subprocess_exec(
                sys.executable, "-m", "worker.main", cwd=ROOT, env=env, stdout=out, stderr=out,
            ))
            out.close()

    async def stop_workers(self):
        for w in self.workers:
            if w.returncode is None:
                w.terminate()
        for w in self.workers:
            try:
                await asyncio.wait_for(w.wait(), timeout=15)
            except asyncio.TimeoutError:
                w.kill()

    async def one_request(self, client, stage, scheduled):
        op = self.rng.choices(self.ops, self.weights)[0]
        if op in ("get", "cancel") and not self.job_ids:
            op = "create"
        if op == "create":
            self.key_seq += 1
            method, path, kwargs = "POST", "/jobs", {
                "json": {"idempotency_key": f"load-{self.run_dir.name}-{self.key_seq}", "payload": {}}}
            ok_codes = (200, 201)
        elif op == "get":
            method, path, kwargs, ok_codes = "GET", f"/jobs/{self.rng.choice(self.job_ids)}", {}, (200,)
        elif op == "list":
            method, path, ok_codes = "GET", "/jobs", (200,)
            kwargs = {"params": {"page": self.rng.randint(1, 10), "limit": 20}}
        else:
            method, path, kwargs = "POST", f"/jobs/{self.rng.choice(self.job_ids)}/cancel", {}
            ok_codes = (200, 409)
        started = time.monotonic()
        try:
            async with client.request(method, path, **kwargs) as r:
                body = await r.json() if r.status == 201 else await r.read()
                if op == "create" and r.status == 201:
                    self.job_ids.append(body["id"])
                outcome = "ok" if r.status in ok_codes else f"http_{r.status}"
        except asyncio.TimeoutError:
            outcome = "timeout"
        except aiohttp.ClientError as exc:
            outcome = type(exc).__name__
        done = time.monotonic()
        self.records.append((stage, op, scheduled - self.t0, done - scheduled, done - started, outcome))

    async def api_cpu_seconds(self) -> float | None:
        if self.api_pid is None:
            return None
        proc = await asyncio.create_subprocess_exec(
            "ps", "-o", "time=", "-p", str(self.api_pid), stdout=asyncio.subprocess.PIPE)
        out = (await proc.communicate())[0].decode().strip()
        if not out:
            return None
        seconds = 0.0
        for part in out.split(":"):  # [[h:]m:]s.ss
            seconds = seconds * 60 + float(part)
        return seconds

    async def sample_loop(self, db):
        last_wall, last_gen, last_api = time.monotonic(), time.process_time(), await self.api_cpu_seconds()
        while not self.stop_sampling.is_set():
            cur = await db.execute(
                "SELECT status::text AS s, COUNT(*) AS n FROM jobs "
                "WHERE status IN ('queued', 'running') GROUP BY status"
            )
            counts = {r["s"]: r["n"] for r in await cur.fetchall()}
            cur = await db.execute(
                "SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE state = 'active') AS active "
                "FROM pg_stat_activity WHERE datname = current_database()"
            )
            conns = await cur.fetchone()
            wall, gen, api = time.monotonic(), time.process_time(), await self.api_cpu_seconds()
            elapsed = max(wall - last_wall, 1e-6)
            gen_cpu = round(100 * (gen - last_gen) / elapsed)
            api_cpu = round(100 * (api - last_api) / elapsed) if api is not None and last_api is not None else None
            last_wall, last_gen, last_api = wall, gen, api
            self.samples.append({
                "generator_cpu_pct": gen_cpu,
                "api_cpu_pct": api_cpu,
                "t": round(time.monotonic() - self.t0, 1),
                "queued": counts.get("queued", 0),
                "running": counts.get("running", 0),
                "db_connections": conns["total"],
                "db_active": conns["active"],
            })
            try:
                await asyncio.wait_for(self.stop_sampling.wait(), timeout=1)
            except asyncio.TimeoutError:
                pass

    async def report_loop(self):
        seen = 0
        while not self.stop_sampling.is_set():
            await asyncio.sleep(5)
            window = self.records[seen:]
            seen += len(window)
            if not window:
                continue
            lat = [r[3] for r in window]
            errors = sum(r[5] != "ok" for r in window)
            s = self.samples[-1] if self.samples else {}
            self.log(f"  last 5s: done={len(window)} ({len(window) / 5:.0f}/s) p50={ms(pct(lat, 50))}ms "
                     f"p99={ms(pct(lat, 99))}ms errors={errors} queued={s.get('queued')} "
                     f"running={s.get('running')} db_conns={s.get('db_connections')} "
                     f"cpu: generator={s.get('generator_cpu_pct')}% api={s.get('api_cpu_pct')}%")

    async def run_stage(self, client, stage, rate, duration, in_flight):
        loop = asyncio.get_running_loop()
        start = loop.time()
        for i in range(int(rate * duration)):
            target = start + i / rate
            delay = target - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
            now = loop.time()
            self.lags[stage].append(now - target)
            scheduled = time.monotonic() - (now - target)
            task = asyncio.create_task(self.one_request(client, stage, scheduled))
            in_flight.add(task)
            task.add_done_callback(in_flight.discard)

    def stage_summary(self, stage, rate, duration) -> dict:
        recs = [r for r in self.records if r[0] == stage]
        start_t = min((r[2] for r in recs), default=0)
        end_t = start_t + duration
        lat_by_op = defaultdict(list)
        for r in recs:
            lat_by_op[r[1]].append(r[3])
        outcomes = Counter(r[5] for r in recs)
        errors = sum(n for o, n in outcomes.items() if o != "ok")
        window = [s for s in self.samples if start_t <= s["t"] <= end_t]
        third = max(1, len(window) // 3)
        backlog_first = sum(s["queued"] for s in window[:third]) / third if window else 0
        backlog_last = sum(s["queued"] for s in window[-third:]) / third if window else 0
        growing = backlog_last > max(20.0, 2 * backlog_first)
        p99s = {op: ms(pct(v, 99)) for op, v in lat_by_op.items()}
        lag = self.lags[stage]
        summary = {
            "stage": stage,
            "target_rps": rate,
            "seconds": duration,
            "requests": len(recs),
            "achieved_rps": round(len(recs) / duration, 1),
            "outcomes": dict(outcomes),
            "error_rate": round(errors / len(recs), 5) if recs else None,
            "latency_ms": {
                op: {"p50": ms(pct(v, 50)), "p95": ms(pct(v, 95)), "p99": p99s[op],
                     "max": ms(max(v)), "n": len(v)}
                for op, v in sorted(lat_by_op.items())
            },
            "generator_lag_ms": {"p50": ms(pct(lag, 50)), "p99": ms(pct(lag, 99)), "max": ms(max(lag, default=0))},
            "queued_avg_first_third": round(backlog_first, 1),
            "queued_avg_last_third": round(backlog_last, 1),
            "queued_max": max((s["queued"] for s in window), default=0),
            "db_connections_max": max((s["db_connections"] for s in window), default=0),
            "generator_cpu_pct": {"avg": round(sum(s["generator_cpu_pct"] for s in window) / len(window))
                                  if window else None,
                                  "max": max((s["generator_cpu_pct"] for s in window), default=None)},
            "api_cpu_pct": {"avg": round(sum(s["api_cpu_pct"] or 0 for s in window) / len(window))
                            if window else None,
                            "max": max((s["api_cpu_pct"] or 0 for s in window), default=None)},
            "backlog_growing": growing,
        }
        summary["passed"] = bool(
            recs
            and summary["error_rate"] < ERROR_LIMIT
            and all(p is not None and p < P99_LIMIT_MS for p in p99s.values())
            and not growing
        )
        return summary

    async def run(self, client) -> bool:
        a = self.args
        async with await psycopg.AsyncConnection.connect(
            a.database_url, autocommit=True, row_factory=dict_row
        ) as db:
            if a.reset:
                await db.execute("TRUNCATE jobs, job_execution_logs")
                self.log("tables truncated (--reset)")
            async with client.get("/health") as r:
                self.log(f"api health: {r.status} {await r.text()}")
                r.raise_for_status()
            await self.start_workers()
            self.log(f"started {a.workers} workers (work {a.work_min_ms}-{a.work_max_ms} ms, "
                     f"fail rate {a.fail_rate}); settling for {a.settle:g}s")
            # Starting 8 Python processes saturates the CPU for a few seconds;
            # load during that window measures process start-up, not the service.
            await asyncio.sleep(a.settle)

            self.t0 = time.monotonic()
            sampler = asyncio.create_task(self.sample_loop(db))
            reporter = asyncio.create_task(self.report_loop())
            in_flight: set = set()
            for stage, (rate, duration) in enumerate(a.stage_list):
                self.log(f"stage {stage}: {rate:g} req/s for {duration:g}s")
                await self.run_stage(client, stage, rate, duration, in_flight)
            self.log(f"load finished; waiting for {len(in_flight)} in-flight requests")
            if in_flight:
                await asyncio.wait(in_flight, timeout=a.timeout + 5)

            drain_start = time.monotonic()
            while time.monotonic() - drain_start < a.drain_wait:
                if self.samples and self.samples[-1]["queued"] == 0 and self.samples[-1]["running"] == 0:
                    break
                await asyncio.sleep(1)
            left = self.samples[-1] if self.samples else {}
            drained_after = round(time.monotonic() - drain_start, 1)
            self.log(f"backlog after load: queued={left.get('queued')} running={left.get('running')} "
                     f"(waited {drained_after}s)")
            self.stop_sampling.set()
            await sampler
            await reporter
            await self.stop_workers()

        stages = [self.stage_summary(i, rate, dur) for i, (rate, dur) in enumerate(a.stage_list)]
        self.log("")
        self.log("=== results per stage ===")
        for s in stages:
            self.log(f"stage {s['stage']}: target {s['target_rps']:g} req/s, achieved {s['achieved_rps']} req/s, "
                     f"{s['requests']} requests -> {'PASS' if s['passed'] else 'FAIL'}")
            self.log(f"  errors: {s['error_rate']:.3%} {s['outcomes']}")
            for op, l in s["latency_ms"].items():
                self.log(f"  {op:7s} n={l['n']:6d} p50={l['p50']}ms p95={l['p95']}ms p99={l['p99']}ms max={l['max']}ms")
            self.log(f"  queue: avg queued first third={s['queued_avg_first_third']} "
                     f"last third={s['queued_avg_last_third']} max={s['queued_max']} "
                     f"growing={s['backlog_growing']}")
            self.log(f"  db connections max={s['db_connections_max']}  "
                     f"generator lag p50={s['generator_lag_ms']['p50']}ms p99={s['generator_lag_ms']['p99']}ms "
                     f"max={s['generator_lag_ms']['max']}ms")
            self.log(f"  cpu: generator avg={s['generator_cpu_pct']['avg']}% max={s['generator_cpu_pct']['max']}%  "
                     f"api avg={s['api_cpu_pct']['avg']}% max={s['api_cpu_pct']['max']}%")
        passed = all(s["passed"] for s in stages)
        self.log("")
        self.log(f"RESULT: {'PASS' if passed else 'FAIL'}")

        (self.run_dir / "summary.json").write_text(json.dumps({
            "run": self.run_dir.name,
            "passed": passed,
            "criteria": {"error_rate_below": ERROR_LIMIT, "p99_ms_below": P99_LIMIT_MS,
                         "backlog": "must not grow"},
            "args": {k: v for k, v in vars(a).items() if k not in ("database_url", "stage_list", "mix_weights")},
            "stages": stages,
            "backlog_after_load": left,
            "drain_wait_s": drained_after,
            "samples": self.samples,
        }, indent=2, default=str))
        return passed


async def wait_for_api(client, proc, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.returncode is not None:
            raise RuntimeError("API process exited during startup, see api.log")
        try:
            async with client.get("/health") as r:
                if r.status == 200:
                    return
        except aiohttp.ClientError:
            pass
        await asyncio.sleep(0.2)
    raise RuntimeError("API did not become healthy, see api.log")


async def main_async(args) -> int:
    run_dir = Path(args.results_dir) / f"load-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    run_dir.mkdir(parents=True)
    log = Log(run_dir / "load.log")
    log(f"run {run_dir.name}: " + " ".join(sys.argv[1:]))

    api = None
    uds, base_url = None, args.api_url
    if args.start_api:
        sock_dir = tempfile.mkdtemp(prefix="jq-", dir="/tmp" if Path("/tmp").is_dir() else None)
        uds, base_url = str(Path(sock_dir) / "api.sock"), "http://api"
        api_log = open(run_dir / "api.log", "w")
        api = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "uvicorn", "app.main:app", "--uds", uds, "--log-level", "warning",
            cwd=ROOT, env={**os.environ, "DATABASE_URL": args.database_url},
            stdout=api_log, stderr=api_log,
        )
    connector = (aiohttp.UnixConnector(path=uds, limit=args.max_connections) if uds
                 else aiohttp.TCPConnector(limit=args.max_connections))
    test = LoadTest(args, run_dir, log)
    test.api_pid = api.pid if api else None
    try:
        async with aiohttp.ClientSession(
            base_url=base_url, connector=connector, timeout=aiohttp.ClientTimeout(total=args.timeout)
        ) as client:
            if api:
                await wait_for_api(client, api)
            passed = await test.run(client)
    except Exception as exc:
        log(f"LOAD TEST ERROR: {type(exc).__name__}: {exc}")
        passed = False
    finally:
        for w in test.workers:
            if w.returncode is None:
                w.kill()
        if api and api.returncode is None:
            api.terminate()
            await api.wait()
        if uds:
            shutil.rmtree(Path(uds).parent, ignore_errors=True)
        log_dir = run_dir / "workers"
        if log_dir.exists():
            shutil.rmtree(log_dir)  # worker logs carry nothing the load test measures
    log(f"raw output: {run_dir}")
    return 0 if passed else 1


def main():
    sys.exit(asyncio.run(main_async(parse_args())))


if __name__ == "__main__":
    main()
