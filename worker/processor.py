from datetime import UTC, datetime
from time import perf_counter

from PIL import Image
from sqlalchemy.orm import Session

from app.config import settings
from app.db.models import TileStatus, TileTask
from app.services.storage import storage_path


def process_tile(db: Session, tile_id: str, worker_id: str) -> TileStatus:
    tile = db.get(TileTask, tile_id)
    if tile is None:
        raise ValueError(f"Tile {tile_id} does not exist")
    if tile.status == TileStatus.COMPLETED:
        return tile.status

    output_path = f"jobs/{tile.job_id}/output/{tile.tile_index}.png"
    if tile.attempt_count >= settings.max_processing_attempts:
        try:
            with Image.open(storage_path(output_path)) as output:
                if output.size != (tile.width, tile.height):
                    raise ValueError(
                        f"expected {tile.width}x{tile.height}, got {output.width}x{output.height}"
                    )
                output.verify()
            tile.output_path = output_path
            tile.status = TileStatus.COMPLETED
            tile.completed_at = datetime.now(UTC)
            tile.error_message = None
        except Exception as exc:
            tile.output_path = None
            tile.status = TileStatus.FAILED
            tile.error_message = (
                "Retry budget exhausted after interrupted processing; "
                f"no valid deterministic output was available: {exc}"
            )
        db.commit()
        return tile.status

    while tile.attempt_count < settings.max_processing_attempts:
        tile.status = TileStatus.PROCESSING
        tile.attempt_count += 1
        tile.worker_id = worker_id
        db.commit()
        started = perf_counter()
        try:
            with Image.open(storage_path(tile.input_path)) as image:
                image.convert("L").save(storage_path(output_path), "PNG")
            tile.output_path = output_path
            tile.status = TileStatus.COMPLETED
            tile.processing_time_ms = round((perf_counter() - started) * 1000)
            tile.completed_at = datetime.now(UTC)
            tile.error_message = None
            break
        except Exception as exc:
            tile.error_message = str(exc)
            if tile.attempt_count == settings.max_processing_attempts:
                tile.status = TileStatus.FAILED
            db.commit()
    db.commit()
    return tile.status
