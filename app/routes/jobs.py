from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Response, status

from app import repository
from app.models import Job, JobCreate, JobPage, JobStatus

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("", response_model=Job, status_code=status.HTTP_201_CREATED)
async def create_job(body: JobCreate, response: Response):
    job, created = await repository.create_job(body.idempotency_key, body.payload)
    if created:
        return job
    # Same key with a different payload is a client bug; returning the old job
    # would silently drop the new request.
    if job["payload"] != body.payload:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "idempotency_key already used with a different payload",
                "job_id": str(job["id"]),
            },
        )
    response.status_code = status.HTTP_200_OK
    return job


@router.get("", response_model=JobPage)
async def list_jobs(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    status: JobStatus | None = None,
):
    items, total = await repository.list_jobs(page, limit, status.value if status else None)
    return {"items": items, "page": page, "limit": limit, "total": total}


@router.get("/{job_id}", response_model=Job)
async def get_job(job_id: UUID):
    job = await repository.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@router.post("/{job_id}/cancel", response_model=Job)
async def cancel_job(job_id: UUID):
    job, cancelled_now = await repository.cancel_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    if not cancelled_now and job["status"] != JobStatus.cancelled.value:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"error": f"job already {job['status']}", "job_id": str(job_id)},
        )
    return job
