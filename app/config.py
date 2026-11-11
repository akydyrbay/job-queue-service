import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    db_pool_min_size: int
    db_pool_max_size: int
    lease_seconds: int


def load_settings() -> Settings:
    return Settings(
        database_url=os.getenv(
            "DATABASE_URL", "postgresql://jobqueue:jobqueue@localhost:5432/jobqueue"
        ),
        db_pool_min_size=int(os.getenv("DB_POOL_MIN_SIZE", "1")),
        db_pool_max_size=int(os.getenv("DB_POOL_MAX_SIZE", "10")),
        lease_seconds=int(os.getenv("LEASE_SECONDS", "10")),
    )


settings = load_settings()
