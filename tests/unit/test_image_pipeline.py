import io
from pathlib import Path

import pytest
from fastapi import HTTPException, UploadFile
from PIL import Image, ImageChops
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from aggregator.runner import aggregate_jobs
from app.api.jobs import _read_image
from app.db.models import Base, ImageJob, JobStatus, TileStatus, TileTask
from app.services.merger import convert_tiles_to_grayscale, merge_tiles
from app.services.splitter import split_image
from app.services.storage import storage_path
from worker.processor import process_tile


@pytest.fixture
def db():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as session:
        yield session
    Base.metadata.drop_all(engine)
    engine.dispose()


def image_bytes(size=(8, 8), image_format="PNG"):
    buffer = io.BytesIO()
    image = Image.new("RGB", size)
    image.putdata(
        [(x % 256, y % 256, (x + y) % 256) for y in range(size[1]) for x in range(size[0])]
    )
    image.save(buffer, image_format)
    return buffer.getvalue()


@pytest.mark.parametrize(
    "filename,content_type,content,status",
    [
        ("bad.txt", "text/plain", b"hello", 415),
        ("bad.png", "image/png", b"not an image", 400),
        ("fake.png", "image/png", image_bytes(image_format="JPEG"), 415),
    ],
)
def test_image_validation_rejects_invalid_uploads(filename, content_type, content, status):
    upload = UploadFile(filename=filename, file=io.BytesIO(content), headers={"content-type": content_type})
    with pytest.raises(HTTPException) as error:
        _read_image(upload, content)
    assert error.value.status_code == status


def test_image_validation_accepts_png():
    content = image_bytes()
    upload = UploadFile(filename="valid.png", file=io.BytesIO(content), headers={"content-type": "image/png"})
    image, extension = _read_image(upload, content)
    assert image.size == (8, 8)
    assert extension == ".png"


@pytest.mark.parametrize(
    "size,expected",
    [
        ((512, 512), [(0, 0, 512, 512)]),
        (
            (1025, 770),
            [
                (0, 0, 512, 512),
                (512, 0, 512, 512),
                (1024, 0, 1, 512),
                (0, 512, 512, 258),
                (512, 512, 512, 258),
                (1024, 512, 1, 258),
            ],
        ),
    ],
)
def test_split_image_records_full_and_edge_tile_coordinates(tmp_path, size, expected):
    tiles = split_image(Image.new("RGB", size), tmp_path)
    assert [(tile.x, tile.y, tile.width, tile.height) for tile in tiles] == expected
    assert [tile.path for tile in tiles] == [tmp_path / f"{index}.png" for index in range(len(tiles))]


def test_out_of_order_reconstruction_is_pixel_exact(tmp_path):
    original = Image.open(io.BytesIO(image_bytes((1025, 770))))
    tiles = split_image(original, tmp_path / "input")
    processed = convert_tiles_to_grayscale(tiles, tmp_path / "output")
    final_path = tmp_path / "final.png"
    merge_tiles(reversed(processed), original.size, final_path)
    with Image.open(final_path) as result:
        assert result.size == original.size
        assert ImageChops.difference(result, original.convert("L")).getbbox() is None


def add_job(db, tmp_path, *, input_exists=True):
    input_path = Path("jobs/job/input/0.png")
    (tmp_path / "jobs" / "job" / "output").mkdir(parents=True)
    if input_exists:
        absolute = tmp_path / input_path
        absolute.parent.mkdir(parents=True)
        Image.new("RGB", (8, 8), "blue").save(absolute)
    job = ImageJob(
        id="job",
        original_filename="input.png",
        original_path="originals/input.png",
        width=8,
        height=8,
        total_tiles=1,
        status=JobStatus.PROCESSING,
    )
    tile = TileTask(
        id="tile",
        job=job,
        tile_index=0,
        x=0,
        y=0,
        width=8,
        height=8,
        input_path=input_path.as_posix(),
    )
    db.add(job)
    db.commit()
    return job, tile


def test_worker_uses_deterministic_output_path_and_state_transitions(db, tmp_path, monkeypatch):
    monkeypatch.setattr("app.config.settings.storage_root", tmp_path)
    job, tile = add_job(db, tmp_path)
    assert tile.status == TileStatus.QUEUED
    assert process_tile(db, tile.id, "worker-one") == TileStatus.COMPLETED
    assert tile.output_path == "jobs/job/output/0.png"
    assert tile.attempt_count == 1
    assert tile.worker_id == "worker-one"
    assert tile.completed_at is not None
    assert aggregate_jobs(db) == 1
    assert job.status == JobStatus.COMPLETED
    assert job.final_path == "final/job.png"


def test_worker_stops_after_three_attempts_and_fails_parent(db, tmp_path, monkeypatch):
    monkeypatch.setattr("app.config.settings.storage_root", tmp_path)
    job, tile = add_job(db, tmp_path, input_exists=False)
    assert process_tile(db, tile.id, "worker-broken") == TileStatus.FAILED
    assert tile.attempt_count == 3
    assert tile.error_message
    assert aggregate_jobs(db) == 0
    assert job.status == JobStatus.FAILED


def test_completed_tile_redelivery_is_skipped(db, tmp_path, monkeypatch):
    monkeypatch.setattr("app.config.settings.storage_root", tmp_path)
    _, tile = add_job(db, tmp_path)
    assert process_tile(db, tile.id, "worker-first") == TileStatus.COMPLETED
    output_path = storage_path(tile.output_path)
    original_bytes = output_path.read_bytes()
    assert process_tile(db, tile.id, "worker-redelivery") == TileStatus.COMPLETED
    assert tile.attempt_count == 1
    assert tile.worker_id == "worker-first"
    assert output_path.read_bytes() == original_bytes
