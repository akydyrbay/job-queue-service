from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from app.db import apply_schema, missing_tables, pool


@asynccontextmanager
async def lifespan(app: FastAPI):
    await pool.open(wait=True)
    await apply_schema()
    yield
    await pool.close()


app = FastAPI(title="Job Queue Service", lifespan=lifespan)


@app.get("/health")
async def health():
    try:
        missing = await missing_tables()
    except Exception as exc:
        return JSONResponse(status_code=503, content={"status": "error", "database": str(exc)})
    if missing:
        return JSONResponse(
            status_code=503, content={"status": "error", "missing_tables": missing}
        )
    return {"status": "ok", "database": "ok"}
