"""Chaos harness: 10,000 jobs, 8 workers, kill -9 about every 2 seconds.

While jobs are being submitted it also sends duplicate POSTs (some racing the
original request, some later) and cancels (some on queued jobs, some on
running ones). When the queue has drained it checks the invariants against
both the database and the API, and writes everything to results/<timestamp>/.

    python -m harness.harness --reset --api-url http://localhost:8000 \\
        --database-url postgresql://jobqueue:jobqueue@localhost:5432/jobqueue

Exit code 0 means every invariant passed.
"""
import argparse
import asyncio
import csv
import gzip
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

import httpx
import psycopg
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parent.parent
TERMINAL = {"succeeded", "failed", "cancelled"}


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--api-url", default="http://localhost:8000")
    p.add_argument("--uds", help="talk to the API over this unix socket instead of TCP")
    p.add_argument("--start-api", action="store_true",
                   help="start uvicorn on a unix socket in the run directory (no Docker needed)")
    p.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    p.add_argument("--reset", action="store_true", help="TRUNCATE jobs before the run")
    p.add_argument("--jobs", type=int, default=10_000)
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--kill-interval", type=float, default=2.0, help="seconds between kills, ±25%%")
    p.add_argument("--lease", type=int, default=5, help="LEASE_SECONDS for the workers")
    p.add_argument("--work-min-ms", type=int, default=20)
    p.add_argument("--work-max-ms", type=int, default=100)
    p.add_argument("--fail-rate", type=float, default=0.05)
    p.add_argument("--concurrency", type=int, default=64, help="parallel submissions")
    p.add_argument("--race-dup-rate", type=float, default=0.1,
                   help="share of jobs submitted twice at the same moment")
    p.add_argument("--dup-senders", type=int, default=4, help="tasks re-posting already acked keys")
    p.add_argument("--cancels", type=int, default=1000, help="how many cancel requests to send")
    p.add_argument("--cancel-senders", type=int, default=12,
                   help="parallel cancel tasks; one task is starved by the submission traffic")
    p.add_argument("--timeout", type=float, default=600, help="max seconds to wait for the queue to drain")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--results-dir", default=str(ROOT / "results"))
    p.add_argument("--note", default="", help="free text stored in results/runs.csv")
    args = p.parse_args(argv)
    if not args.database_url:
        p.error("--database-url or DATABASE_URL is required")
    return args


class Log:
    def __init__(self, path: Path):
        self.file = open(path, "w", buffering=1)
        self.t0 = time.monotonic()

    def __call__(self, msg: str = ""):
        line = f"[{time.monotonic() - self.t0:7.1f}s] {msg}"
        print(line, flush=True)
        self.file.write(line + "\n")

    def elapsed(self) -> float:
        return time.monotonic() - self.t0


class Harness:
    def __init__(self, args, run_dir: Path, log: Log):
        self.args = args
        self.run_dir = run_dir
        self.log = log
        self.rng = random.Random(args.seed)
        self.keys = [f"chaos-{run_dir.name}-{i:05d}" for i in range(args.jobs)]

        # key -> every (status_code, job_id) the API returned for it
        self.post_results: dict[str, list[tuple[int, str | None]]] = defaultdict(list)
        self.acked: dict[str, str] = {}
        self.acked_keys: list[str] = []
        self.unacked: set[str] = set()
        self.race_dups = 0
        self.late_dups = 0
        self.http_errors = Counter()

        # job_id -> every (status_code, returned status) for cancel calls
        self.cancel_results: dict[str, list[tuple[int, str | None]]] = defaultdict(list)
        self.cancel_unknown: set[str] = set()  # cancel request that never got an answer
        self.cancels_sent = 0
        self.cancels_from_running_list = 0

        self.workers: dict[int, asyncio.subprocess.Process] = {}
        self.all_worker_pids: set[int] = set()
        self.worker_seq = 0
        self.kills: list[dict] = []

        self.submit_done = asyncio.Event()
        self.drained = asyncio.Event()
        self.stop_killing = asyncio.Event()

    # processes

    async def start_worker(self):
        self.worker_seq += 1
        log_dir = self.run_dir / "workers"
        log_dir.mkdir(exist_ok=True)
        env = {
            **os.environ,
            "DATABASE_URL": self.args.database_url,
            "LEASE_SECONDS": str(self.args.lease),
            "WORK_MIN_MS": str(self.args.work_min_ms),
            "WORK_MAX_MS": str(self.args.work_max_ms),
            "FAIL_RATE": str(self.args.fail_rate),
            "PYTHONUNBUFFERED": "1",
        }
        out = open(log_dir / f"worker-{self.worker_seq:03d}.log", "w")
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "worker.main", cwd=ROOT, env=env, stdout=out, stderr=out,
        )
        out.close()
        self.workers[proc.pid] = proc
        self.all_worker_pids.add(proc.pid)
        return proc

    async def kill_loop(self, db: psycopg.AsyncConnection):
        while not self.stop_killing.is_set():
            delay = self.args.kill_interval * self.rng.uniform(0.75, 1.25)
            try:
                await asyncio.wait_for(self.stop_killing.wait(), timeout=delay)
                return
            except asyncio.TimeoutError:
                pass
            pid = self.rng.choice(list(self.workers))
            cur = await db.execute(
                "SELECT id FROM jobs WHERE status = 'running' AND worker_id LIKE %s",
                (f"%-{pid}-%",),
            )
            held = [str(r["id"]) for r in await cur.fetchall()]
            proc = self.workers.pop(pid)
            proc.kill() 
            await proc.wait()
            replacement = await self.start_worker()
            self.kills.append({"t": round(self.log.elapsed(), 2), "pid": pid, "held": held,
                               "replacement": replacement.pid})
            self.log(f"kill -9 pid={pid} holding={held[0] if held else '-'} "
                     f"-> started pid={replacement.pid} (kill #{len(self.kills)})")

    async def stop_workers(self):
        for proc in self.workers.values():
            if proc.returncode is None:
                proc.terminate()
        codes = []
        for proc in self.workers.values():
            try:
                codes.append(await asyncio.wait_for(proc.wait(), timeout=15))
            except asyncio.TimeoutError:
                proc.kill()
                codes.append("killed")
        return codes

    # traffic

    async def post_job(self, client: httpx.AsyncClient, key: str):
        body = {"idempotency_key": key, "payload": {"key": key}}
        for attempt in range(6):
            try:
                r = await client.post("/jobs", json=body)
            except httpx.TransportError as exc:
                self.http_errors[f"POST {type(exc).__name__}"] += 1
                await asyncio.sleep(0.1 * 2 ** attempt)
                continue
            job_id = r.json().get("id") if r.status_code in (200, 201) else None
            self.post_results[key].append((r.status_code, job_id))
            if r.status_code >= 500:
                self.http_errors[f"POST {r.status_code}"] += 1
                await asyncio.sleep(0.1 * 2 ** attempt)
                continue
            if job_id and key not in self.acked:
                self.acked[key] = job_id
                self.acked_keys.append(key)
            return
        self.unacked.add(key)

    async def submit_all(self, client):
        sem = asyncio.Semaphore(self.args.concurrency)

        async def one(key):
            async with sem:
                if self.rng.random() < self.args.race_dup_rate:
                    self.race_dups += 1
                    await asyncio.gather(self.post_job(client, key), self.post_job(client, key))
                else:
                    await self.post_job(client, key)

        await asyncio.gather(*(one(k) for k in self.keys))
        self.submit_done.set()
        self.log(f"submission done: {len(self.acked)}/{len(self.keys)} acked, "
                 f"{self.race_dups} racing duplicates, {self.late_dups} late duplicates so far")

    async def duplicate_loop(self, client):
        while not self.submit_done.is_set():
            if self.acked_keys:
                self.late_dups += 1
                await self.post_job(client, self.rng.choice(self.acked_keys))
            await asyncio.sleep(0.01)

    async def cancel_one(self, client, job_id: str):
        self.cancels_sent += 1
        for attempt in range(6):
            try:
                r = await client.post(f"/jobs/{job_id}/cancel")
            except httpx.TransportError as exc:
                self.http_errors[f"CANCEL {type(exc).__name__}"] += 1
                await asyncio.sleep(0.1 * 2 ** attempt)
                continue
            status = r.json().get("status") if r.status_code == 200 else None
            self.cancel_results[job_id].append((r.status_code, status))
            if r.status_code >= 500:
                self.http_errors[f"CANCEL {r.status_code}"] += 1
                continue
            return
        self.cancel_unknown.add(job_id)

    async def cancel_loop(self, client):
        # Spread cancels over roughly the submission window, then keep going until the target is reached or there is nothing left to cancel.
        while self.cancels_sent < self.args.cancels and not self.drained.is_set():
            job_id = None
            if self.rng.random() < 0.5:
                try:
                    r = await client.get("/jobs", params={"status": "running", "limit": 50})
                    running = [j["id"] for j in r.json()["items"]]
                except (httpx.TransportError, KeyError, ValueError):
                    running = []
                if running:
                    job_id = self.rng.choice(running)
                    self.cancels_from_running_list += 1
            if job_id is None and self.acked_keys:
                job_id = self.acked[self.rng.choice(self.acked_keys)]
            if job_id:
                await self.cancel_one(client, job_id)
            await asyncio.sleep(0.02)

    # waiting

    async def status_counts(self, db) -> Counter:
        cur = await db.execute("SELECT status::text AS s, COUNT(*) AS n FROM jobs GROUP BY status")
        return Counter({r["s"]: r["n"] for r in await cur.fetchall()})

    async def wait_drained(self, db) -> bool:
        await self.submit_done.wait()
        deadline = time.monotonic() + self.args.timeout
        last_report = 0.0
        while True:
            counts = await self.status_counts(db)
            if counts["queued"] == 0 and counts["running"] == 0:
                self.drained.set()
                return True
            if time.monotonic() > deadline:
                self.log(f"TIMEOUT after {self.args.timeout}s waiting for the queue to drain: {dict(counts)}")
                self.drained.set()
                return False
            if time.monotonic() - last_report > 5:
                last_report = time.monotonic()
                self.log(f"progress {dict(sorted(counts.items()))} kills={len(self.kills)}")
            await asyncio.sleep(0.5)

    async def progress_during_submit(self, db):
        while not self.submit_done.is_set():
            await asyncio.sleep(5)
            counts = await self.status_counts(db)
            self.log(f"submitting: acked={len(self.acked)} late_dups={self.late_dups} "
                     f"cancels={self.cancels_sent} db={dict(sorted(counts.items()))} kills={len(self.kills)}")

    # invariants

    async def check(self, db, client, drained_in_time: bool) -> list[tuple[str, bool, str]]:
        results = []

        def record(name, ok, detail):
            results.append((name, ok, detail))
            self.log(f"{'PASS' if ok else 'FAIL'}  {name}: {detail}")

        cur = await db.execute(
            "SELECT id::text, idempotency_key, status::text, attempts, worker_id, last_error FROM jobs"
        )
        jobs = {r["id"]: r for r in await cur.fetchall()}
        by_key = {j["idempotency_key"]: j for j in jobs.values()}
        cur = await db.execute(
            "SELECT job_id::text, worker_id, attempt FROM job_execution_logs ORDER BY id"
        )
        effects = defaultdict(list)
        for r in await cur.fetchall():
            effects[r["job_id"]].append(r)
        status_counts = Counter(j["status"] for j in jobs.values())

        # 1. No job lost
        missing = [k for k in self.keys if k not in by_key]
        wrong_id = [k for k, jid in self.acked.items() if k in by_key and by_key[k]["id"] != jid]
        acked_missing = [k for k in self.acked if k not in by_key]
        ok = len(jobs) == len(self.keys) and not missing and not wrong_id
        record("1 no job lost", ok,
               f"db rows={len(jobs)} expected={len(self.keys)} missing_keys={len(missing)} "
               f"acked_but_missing={len(acked_missing)} acked_id_mismatch={len(wrong_id)} "
               f"never_acked_by_api={len(self.unacked)}"
               + (f" e.g. {(missing or wrong_id)[:5]}" if missing or wrong_id else ""))

        # 2. At most one side effect per job
        dup_effects = {jid: len(rows) for jid, rows in effects.items() if len(rows) > 1}
        record("2 at most one side effect", not dup_effects,
               f"jobs with >1 effect={len(dup_effects)}"
               + (f" e.g. {list(dup_effects.items())[:5]}" if dup_effects else ""))

        # 3. Effect matches final state, and comes from the final owner
        bad_state = [jid for jid, j in jobs.items()
                     if (j["status"] == "succeeded") != (len(effects.get(jid, [])) == 1)]
        orphan_effects = [jid for jid in effects if jid not in jobs]
        wrong_owner = [jid for jid, j in jobs.items() if j["status"] == "succeeded" and effects.get(jid)
                       and (effects[jid][0]["worker_id"], effects[jid][0]["attempt"])
                       != (j["worker_id"], j["attempts"])]
        record("3 effect matches state", not bad_state and not orphan_effects and not wrong_owner,
               f"succeeded_without_1_effect_or_other_with_effect={len(bad_state)} "
               f"effects_for_unknown_jobs={len(orphan_effects)} effect_not_from_final_attempt={len(wrong_owner)}"
               + (f" e.g. {(bad_state or wrong_owner)[:5]}" if bad_state or wrong_owner else ""))

        # 4. Nothing stuck
        stuck = [jid for jid, j in jobs.items() if j["status"] not in TERMINAL]
        record("4 none stuck in queued/running", drained_in_time and not stuck,
               f"drained_in_time={drained_in_time} queued={status_counts['queued']} "
               f"running={status_counts['running']}")

        # 5. Consistent final state: duplicates, API vs DB
        split_ids = [k for k, rs in self.post_results.items() if len({jid for _, jid in rs if jid}) > 1]
        multi_created = [k for k, rs in self.post_results.items() if sum(c == 201 for c, _ in rs) > 1]
        conflicts = sum(c == 409 for rs in self.post_results.values() for c, _ in rs)
        sample = self.rng.sample(list(jobs), min(500, len(jobs)))
        sample += [jid for jid in self.cancel_results if jid in jobs]
        mismatched = await self.compare_api_to_db(client, jobs, sample)
        list_mismatch = await self.compare_list_totals(client, status_counts, len(jobs))
        ok = not split_ids and not multi_created and conflicts == 0 and not mismatched and not list_mismatch
        record("5 consistent final state", ok,
               f"keys_with_different_ids={len(split_ids)} keys_created_twice={len(multi_created)} "
               f"unexpected_409={conflicts} api_vs_db_mismatch={len(mismatched)}/{len(set(sample))} "
               f"list_totals_mismatch={list_mismatch or 'none'}"
               + (f" e.g. {(split_ids or multi_created or mismatched)[:5]}"
                  if split_ids or multi_created or mismatched else ""))

        # 6. Cancels respected
        accepted = {jid for jid, rs in self.cancel_results.items() if any(c == 200 for c, _ in rs)}
        rejected = {jid for jid, rs in self.cancel_results.items() if all(c == 409 for c, _ in rs)}
        not_cancelled = [jid for jid in accepted if jobs[jid]["status"] != "cancelled"]
        bad_409 = [jid for jid in rejected if jobs[jid]["status"] not in ("succeeded", "failed")]
        unexplained = [jid for jid, j in jobs.items() if j["status"] == "cancelled"
                       and jid not in accepted and jid not in self.cancel_unknown]
        record("6 cancels respected", not not_cancelled and not bad_409 and not unexplained,
               f"cancel_200={len(accepted)} cancel_409={len(rejected)} "
               f"accepted_but_not_cancelled={len(not_cancelled)} 409_but_not_finished={len(bad_409)} "
               f"cancelled_without_a_cancel_request={len(unexplained)}"
               + (f" e.g. {(not_cancelled or bad_409 or unexplained)[:5]}"
                  if not_cancelled or bad_409 or unexplained else ""))

        # 7. Only the harness's own workers touched the jobs
        foreign = {j["worker_id"] for j in jobs.values() if j["worker_id"]
                   and int(j["worker_id"].split("-")[-2]) not in self.all_worker_pids}
        record("7 only harness workers ran jobs", not foreign,
               f"foreign_worker_ids={sorted(foreign)[:5] if foreign else 'none'}")

        # 8. No double claims. A worker holds one job at a time, so each kill
        # can orphan at most one job, and with jobs shorter than the lease
        # nothing else should cause a reclaim. Extra claims beyond the number of
        # kills mean two workers claimed the same job. The guarded finish hides
        # that from invariants 2-3 (the loser is rejected), so check it here.
        extra_claims = sum(max(j["attempts"] - 1, 0) for j in jobs.values())
        record("8 no double claims", extra_claims <= len(self.kills),
               f"extra_claims={extra_claims} kills={len(self.kills)} (extra claims must be <= kills)")

        return results

    async def compare_api_to_db(self, client, jobs, ids) -> list[str]:
        sem = asyncio.Semaphore(32)
        mismatched = []

        async def one(jid):
            async with sem:
                try:
                    r = await client.get(f"/jobs/{jid}")
                    api = r.json()
                except (httpx.TransportError, ValueError):
                    mismatched.append(jid)
                    return
                db = jobs[jid]
                if r.status_code != 200 or (api["status"], api["attempts"], api["worker_id"]) != (
                    db["status"], db["attempts"], db["worker_id"]
                ):
                    mismatched.append(jid)

        await asyncio.gather(*(one(j) for j in set(ids)))
        return mismatched

    async def compare_list_totals(self, client, status_counts, total) -> dict:
        diff = {}
        r = await client.get("/jobs", params={"limit": 1})
        if r.json()["total"] != total:
            diff["all"] = (r.json()["total"], total)
        for status in ("queued", "running", "succeeded", "failed", "cancelled"):
            r = await client.get("/jobs", params={"status": status, "limit": 1})
            if r.json()["total"] != status_counts[status]:
                diff[status] = (r.json()["total"], status_counts[status])
        return diff

    async def stats(self, db) -> dict:
        cur = await db.execute(
            "SELECT COUNT(*) FILTER (WHERE attempts > 1) AS reclaimed, MAX(attempts) AS max_attempts, "
            "COUNT(*) FILTER (WHERE status = 'cancelled' AND worker_id IS NOT NULL) AS cancelled_after_claim, "
            "COUNT(*) FILTER (WHERE last_error LIKE 'gave up%%') AS gave_up FROM jobs"
        )
        row = await cur.fetchone()
        events = Counter()
        for f in (self.run_dir / "workers").glob("*.log"):
            for line in f.read_text().splitlines():
                if "event=" in line:
                    events[line.split("event=")[1].split()[0]] += 1
        held = [k for k in self.kills if k["held"]]
        return {
            "jobs_reclaimed_after_lease_expiry": row["reclaimed"],
            "max_attempts_seen": row["max_attempts"],
            "jobs_cancelled_after_being_claimed": row["cancelled_after_claim"],
            "jobs_given_up": row["gave_up"],
            "kills": len(self.kills),
            "kills_while_holding_a_job": len(held),
            "racing_duplicate_posts": self.race_dups,
            "late_duplicate_posts": self.late_dups,
            "cancel_requests": self.cancels_sent,
            "cancels_aimed_at_running_jobs": self.cancels_from_running_list,
            "http_errors": dict(self.http_errors),
            "worker_events": dict(events),
        }

    # run

    async def run(self, client: httpx.AsyncClient) -> bool:
        a = self.args
        async with await psycopg.AsyncConnection.connect(
            a.database_url, autocommit=True, row_factory=dict_row
        ) as db:
            cur = await db.execute("SELECT COUNT(*) AS n FROM jobs")
            existing = (await cur.fetchone())["n"]
            if existing and not a.reset:
                self.log(f"jobs table already has {existing} rows; rerun with --reset to clear it")
                return False
            if a.reset:
                await db.execute("TRUNCATE jobs, job_execution_logs")
                self.log("tables truncated (--reset)")

            r = await client.get("/health")
            self.log(f"api health: {r.status_code} {r.text}")
            r.raise_for_status()

            for _ in range(a.workers):
                await self.start_worker()
            self.log(f"started {a.workers} workers: {sorted(self.workers)}")

            killer = asyncio.create_task(self.kill_loop(db))
            started = time.monotonic()
            traffic = [
                asyncio.create_task(self.submit_all(client)),
                *(asyncio.create_task(self.cancel_loop(client)) for _ in range(a.cancel_senders)),
                asyncio.create_task(self.progress_during_submit(db)),
                *(asyncio.create_task(self.duplicate_loop(client)) for _ in range(a.dup_senders)),
            ]
            async with await psycopg.AsyncConnection.connect(
                a.database_url, autocommit=True, row_factory=dict_row
            ) as watch:
                drained = await self.wait_drained(watch)
            duration = time.monotonic() - started
            self.log(f"queue {'drained' if drained else 'NOT drained'} after {duration:.1f}s")

            self.stop_killing.set()
            await killer
            await asyncio.gather(*traffic)
            exit_codes = await self.stop_workers()
            self.log(f"workers stopped with SIGTERM, exit codes: {Counter(exit_codes)}")

            self.log("")
            self.log("=== invariants ===")
            results = await self.check(db, client, drained)
            stats = await self.stats(db)
            stats["duration_s"] = round(duration, 1)
            stats["final_status_counts"] = dict(await self.status_counts(db))

        passed = all(ok for _, ok, _ in results)
        self.log("")
        self.log("=== stats ===")
        for k, v in stats.items():
            self.log(f"{k}: {v}")
        self.log("")
        self.log(f"RESULT: {'PASS' if passed else 'FAIL'}")

        (self.run_dir / "summary.json").write_text(json.dumps({
            "run": self.run_dir.name,
            "passed": passed,
            "args": {k: v for k, v in vars(a).items() if k != "database_url"},
            "invariants": [{"name": n, "passed": ok, "detail": d} for n, ok, d in results],
            "stats": stats,
            "kills": self.kills,
        }, indent=2, default=str))
        self.append_runs_csv(passed, stats)
        return passed

    def append_runs_csv(self, passed: bool, stats: dict):
        path = Path(self.args.results_dir) / "runs.csv"
        new = not path.exists()
        with open(path, "a", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["run", "result", "jobs", "workers", "kills", "reclaimed", "duration_s",
                            "final_status_counts", "note"])
            w.writerow([self.run_dir.name, "PASS" if passed else "FAIL", self.args.jobs, self.args.workers,
                        stats["kills"], stats["jobs_reclaimed_after_lease_expiry"], stats["duration_s"],
                        json.dumps(stats["final_status_counts"], sort_keys=True), self.args.note])

    def compress_worker_logs(self):
        """Merge per-worker logs into one gzip so results stay small enough to commit."""
        log_dir = self.run_dir / "workers"
        if not log_dir.exists():
            return
        with gzip.open(self.run_dir / "workers.log.gz", "wt") as out:
            for f in sorted(log_dir.glob("*.log")):
                out.write(f"===== {f.name}\n{f.read_text()}")
        shutil.rmtree(log_dir)


async def wait_for_api(client, proc, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.returncode is not None:
            raise RuntimeError("API process exited during startup, see api.log")
        try:
            if (await client.get("/health")).status_code == 200:
                return
        except httpx.TransportError:
            pass
        await asyncio.sleep(0.2)
    raise RuntimeError("API did not become healthy, see api.log")


async def main_async(args) -> int:
    run_dir = Path(args.results_dir) / datetime.now().strftime("%Y%m%d-%H%M%S")
    run_dir.mkdir(parents=True)
    log = Log(run_dir / "harness.log")
    log(f"run {run_dir.name}: " + " ".join(sys.argv[1:]))

    api = None
    uds, base_url = args.uds, args.api_url
    if args.start_api:
        # unix socket paths are limited to ~100 bytes, so keep it out of run_dir
        sock_dir = tempfile.mkdtemp(prefix="jq-", dir="/tmp" if Path("/tmp").is_dir() else None)
        uds, base_url = str(Path(sock_dir) / "api.sock"), "http://api"
        api_log = open(run_dir / "api.log", "w")
        api = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "uvicorn", "app.main:app", "--uds", uds, "--log-level", "warning",
            cwd=ROOT, env={**os.environ, "DATABASE_URL": args.database_url},
            stdout=api_log, stderr=api_log,
        )
    transport = httpx.AsyncHTTPTransport(uds=uds) if uds else None
    limits = httpx.Limits(max_connections=args.concurrency * 2 + 32)
    harness = Harness(args, run_dir, log)
    try:
        async with httpx.AsyncClient(base_url=base_url, transport=transport, limits=limits,
                                     timeout=30) as client:
            if api:
                await wait_for_api(client, api)
            passed = await harness.run(client)
    except Exception as exc:
        log(f"HARNESS ERROR: {type(exc).__name__}: {exc}")
        log("RESULT: FAIL")
        passed = False
    finally:
        for proc in harness.workers.values():
            if proc.returncode is None:
                proc.kill()
        if api and api.returncode is None:
            api.terminate()
            await api.wait()
        harness.compress_worker_logs()
        if uds and args.start_api:
            shutil.rmtree(Path(uds).parent, ignore_errors=True)
    log(f"raw output: {run_dir}")
    return 0 if passed else 1


def main():
    sys.exit(asyncio.run(main_async(parse_args())))


if __name__ == "__main__":
    main()
