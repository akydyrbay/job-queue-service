import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    db_pool_min_size: int
    db_pool_max_size: int
    lease_seconds: int
    # worker only
    max_attempts: int
    fail_rate: float
    work_min_ms: int
    work_max_ms: int
    poll_interval_ms: int


def load_settings() -> Settings:
    return Settings(
        database_url=os.getenv(
            "DATABASE_URL", "postgresql://jobqueue:jobqueue@localhost:5432/jobqueue"
        ),
        db_pool_min_size=int(os.getenv("DB_POOL_MIN_SIZE", "1")),
        db_pool_max_size=int(os.getenv("DB_POOL_MAX_SIZE", "10")),
        lease_seconds=int(os.getenv("LEASE_SECONDS", "10")),
        max_attempts=int(os.getenv("MAX_ATTEMPTS", "5")),
        fail_rate=float(os.getenv("FAIL_RATE", "0")),
        work_min_ms=int(os.getenv("WORK_MIN_MS", "50")),
        work_max_ms=int(os.getenv("WORK_MAX_MS", "300")),
        poll_interval_ms=int(os.getenv("POLL_INTERVAL_MS", "100")),
    )


settings = load_settings()
