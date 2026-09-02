import io
import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, UploadFile
from fastapi.responses import FileResponse, RedirectResponse
from PIL import Image, UnidentifiedImageError
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.config import settings
from app.db.models import ImageJob, JobStatus, TileStatus, TileTask
from app.db.session import get_db
from messaging.producer import publish_tile_jobs
from app.services.splitter import split_image
from app.services.storage import prepare_job_directories, storage_path

router = APIRouter()
ALLOWED_TYPES = {"image/jpeg": ".jpg", "image/png": ".png"}


def _read_image(upload: UploadFile, content: bytes) -> tuple[Image.Image, str]:
    extension = Path(upload.filename or "").suffix.lower()
    expected_extension = ALLOWED_TYPES.get(upload.content_type or "")
    if expected_extension is None or extension not in {".jpg", ".jpeg", ".png"}:
        raise HTTPException(415, "Only JPEG and PNG images are supported")
    if expected_extension == ".png" and extension != ".png":
        raise HTTPException(415, "File extension does not match its MIME type")
    if expected_extension == ".jpg" and extension not in {".jpg", ".jpeg"}:
        raise HTTPException(415, "File extension does not match its MIME type")
    try:
        image = Image.open(io.BytesIO(content))
        image.load()
    except (UnidentifiedImageError, OSError) as exc:
        raise HTTPException(400, "The uploaded file is not a valid image") from exc
    if image.format != ("PNG" if expected_extension == ".png" else "JPEG"):
        raise HTTPException(415, "Image content does not match its MIME type")
    return image, expected_extension


@router.post("/jobs")
def create_job(upload: UploadFile, db: Session = Depends(get_db)):
    content = upload.file.read(settings.max_upload_size_mb * 1024 * 1024 + 1)
    if len(content) > settings.max_upload_size_mb * 1024 * 1024:
        raise HTTPException(413, f"Image exceeds the {settings.max_upload_size_mb} MB limit")
    image, extension = _read_image(upload, content)
    job_id = str(uuid.uuid4())
    prepare_job_directories(job_id)
    original_relative = f"originals/{job_id}{extension}"
    storage_path(original_relative).write_bytes(content)
    job = ImageJob(
        id=job_id,
        original_filename=upload.filename or f"upload{extension}",
        original_path=original_relative,
        width=image.width,
        height=image.height,
        tile_size=settings.tile_size,
        status=JobStatus.SPLITTING,
    )
    db.add(job)
    try:
        tiles = split_image(image, storage_path(f"jobs/{job_id}/input"), settings.tile_size)
        for tile in tiles:
            db.add(TileTask(
                job_id=job_id,
                tile_index=tile.index,
                x=tile.x,
                y=tile.y,
                width=tile.width,
                height=tile.height,
                input_path=f"jobs/{job_id}/input/{tile.index}.png",
            ))
        job.total_tiles = len(tiles)
        job.status = JobStatus.PROCESSING
        db.commit()

        tile_rows = db.scalars(
            select(TileTask).where(TileTask.job_id == job_id).order_by(TileTask.tile_index)
        ).all()
        publish_tile_jobs(tile_rows)
    except Exception as exc:
        db.rollback()
        persisted_job = db.get(ImageJob, job_id)
        if persisted_job:
            persisted_job.status = JobStatus.FAILED
            persisted_job.error_message = str(exc)
            db.commit()
        raise HTTPException(500, "Image processing failed") from exc
    return RedirectResponse(f"/jobs/{job_id}/view", status_code=303)


@router.get("/jobs/{job_id}")
def job_status(job_id: str, db: Session = Depends(get_db)):
    job = db.get(ImageJob, job_id)
    if job is None:
        raise HTTPException(404, "Job not found")
    counts = dict(db.execute(
        select(TileTask.status, func.count()).where(TileTask.job_id == job_id).group_by(TileTask.status)
    ).all())
    completed = counts.get(TileStatus.COMPLETED, 0)
    return {
        "job_id": job.id,
        "status": job.status.value,
        "completed_tiles": completed,
        "failed_tiles": counts.get(TileStatus.FAILED, 0),
        "total_tiles": job.total_tiles,
        "progress_percent": round(completed / job.total_tiles * 100) if job.total_tiles else 0,
    }


@router.get("/jobs/{job_id}/download")
def download(job_id: str, db: Session = Depends(get_db)):
    job = db.get(ImageJob, job_id)
    if job is None:
        raise HTTPException(404, "Job not found")
    if job.status != JobStatus.COMPLETED or not job.final_path:
        raise HTTPException(409, "Job is not complete")
    return FileResponse(storage_path(job.final_path), media_type="image/png", filename=f"{job_id}.png")
