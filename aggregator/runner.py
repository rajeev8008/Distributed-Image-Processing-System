import logging
from datetime import UTC, datetime
from time import sleep

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import settings
from app.db.models import ImageJob, JobStatus, TileStatus, TileTask
from app.db.session import SessionLocal
from app.services.merger import merge_tiles
from app.services.splitter import TileInfo
from app.services.storage import storage_path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def aggregate_jobs(db: Session) -> int:
    merged = 0
    jobs = db.scalars(select(ImageJob).where(ImageJob.status == JobStatus.PROCESSING)).all()
    for job in jobs:
        tiles = db.scalars(select(TileTask).where(TileTask.job_id == job.id)).all()
        if any(tile.status == TileStatus.FAILED for tile in tiles):
            job.status = JobStatus.FAILED
            job.error_message = "One or more tiles failed"
            db.commit()
            continue
        if len(tiles) != job.total_tiles or any(tile.status != TileStatus.COMPLETED for tile in tiles):
            continue

        job_id = job.id
        try:
            if any(not tile.output_path for tile in tiles):
                raise ValueError("Completed tile is missing its output path")
            job.status = JobStatus.MERGING
            db.flush()
            final_path = f"final/{job_id}.png"
            merge_tiles(
                (
                    TileInfo(tile.tile_index, tile.x, tile.y, tile.width, tile.height, storage_path(tile.output_path))
                    for tile in tiles
                ),
                (job.width, job.height),
                storage_path(final_path),
            )
            job.final_path = final_path
            job.status = JobStatus.COMPLETED
            job.completed_at = datetime.now(UTC)
            db.commit()
            merged += 1
            logger.info("event=job_merged job_id=%s tiles=%s", job_id, job.total_tiles)
        except Exception as exc:
            db.rollback()
            failed_job = db.get(ImageJob, job_id)
            if failed_job:
                failed_job.status = JobStatus.FAILED
                failed_job.error_message = str(exc)
                db.commit()
            logger.exception("event=merge_failed job_id=%s", job_id)
    return merged


def run() -> None:
    logger.info("event=aggregator_started interval_seconds=%s", settings.aggregator_interval_seconds)
    while True:
        with SessionLocal() as db:
            aggregate_jobs(db)
        sleep(settings.aggregator_interval_seconds)


if __name__ == "__main__":
    run()
