-- Job queue schema (PostgreSQL 13+; gen_random_uuid() is built in).
-- Safe to re-run: every object is created only if missing.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'job_status') THEN
        CREATE TYPE job_status AS ENUM ('queued', 'running', 'succeeded', 'failed', 'cancelled');
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS jobs (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- UNIQUE makes client retries collapse onto one row at the DB level,
    -- so two concurrent POSTs with the same key can never both insert.
    idempotency_key  VARCHAR(255) NOT NULL UNIQUE,
    payload          JSONB        NOT NULL DEFAULT '{}'::jsonb,
    status           job_status   NOT NULL DEFAULT 'queued',

    -- Lease: who holds the job and until when. Always set from the DB clock
    -- (NOW()), never the worker's clock, so clock skew cannot extend a lease.
    worker_id        VARCHAR(255),
    lease_expires_at TIMESTAMPTZ,
    -- Bumped on every claim; > 1 means the job was reclaimed after a lease expired.
    attempts         INTEGER      NOT NULL DEFAULT 0,

    result_output    TEXT,
    last_error       TEXT,

    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    finished_at      TIMESTAMPTZ,

    -- A running job must always have an owner and a lease, otherwise it could
    -- never be recovered by lease expiry and would be stuck forever.
    CONSTRAINT running_has_lease CHECK (
        status <> 'running' OR (worker_id IS NOT NULL AND lease_expires_at IS NOT NULL)
    ),
    CONSTRAINT attempts_non_negative CHECK (attempts >= 0)
);

-- Side-effect ledger used by the harness invariant check
-- ("no job has more than one execution").
-- Deliberately NOT unique on job_id: a duplicate execution must show up as a
-- second row the harness can count, not be hidden by a constraint error.
CREATE TABLE IF NOT EXISTS job_execution_logs (
    id             BIGSERIAL    PRIMARY KEY,
    job_id         UUID         NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    worker_id      VARCHAR(255) NOT NULL,
    attempt        INTEGER      NOT NULL,
    execution_time TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Claim path, part 1: oldest queued job first (FIFO).
CREATE INDEX IF NOT EXISTS idx_jobs_queued
    ON jobs (created_at) WHERE status = 'queued';

-- Claim path, part 2: running jobs whose lease has expired (dead-worker recovery).
CREATE INDEX IF NOT EXISTS idx_jobs_running_lease
    ON jobs (lease_expires_at) WHERE status = 'running';

-- GET /jobs pagination, newest first, with id as a stable tie-breaker.
CREATE INDEX IF NOT EXISTS idx_jobs_created_at
    ON jobs (created_at DESC, id);

-- Invariant check: GROUP BY job_id HAVING COUNT(*) > 1.
CREATE INDEX IF NOT EXISTS idx_job_execution_logs_job_id
    ON job_execution_logs (job_id);

CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_jobs_updated_at ON jobs;
CREATE TRIGGER trg_jobs_updated_at
    BEFORE UPDATE ON jobs
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
