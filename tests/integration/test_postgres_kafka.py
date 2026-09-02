import json
import os
import time
import uuid
from pathlib import Path

import pytest
from confluent_kafka import Consumer
from PIL import Image, ImageChops
from sqlalchemy import create_engine, delete
from sqlalchemy.orm import Session, sessionmaker

from aggregator.runner import aggregate_jobs
from app.db.models import Base, ImageJob, JobStatus, TileStatus, TileTask
from messaging.producer import _producer, publish_tile_jobs
from worker.consumer import handle_message


DATABASE_URL = os.getenv("TEST_DATABASE_URL")
KAFKA_SERVERS = os.getenv("TEST_KAFKA_BOOTSTRAP_SERVERS")
pytestmark = pytest.mark.skipif(
    not DATABASE_URL or not KAFKA_SERVERS,
    reason="TEST_DATABASE_URL and TEST_KAFKA_BOOTSTRAP_SERVERS are required",
)


@pytest.fixture
def session_factory():
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    yield factory
    with factory() as db:
        db.execute(delete(TileTask))
        db.execute(delete(ImageJob))
        db.commit()
    engine.dispose()


def make_job(db, tile_count=1, *, job_id=None):
    job = ImageJob(
        id=job_id or str(uuid.uuid4()),
        original_filename="integration.png",
        original_path="originals/integration.png",
        width=tile_count * 8,
        height=8,
        total_tiles=tile_count,
        status=JobStatus.PROCESSING,
    )
    for index in range(tile_count):
        job.tiles.append(
            TileTask(
                id=str(uuid.uuid4()),
                tile_index=index,
                x=index * 8,
                y=0,
                width=8,
                height=8,
                input_path=f"jobs/{job.id}/input/{index}.png",
            )
        )
    db.add(job)
    db.commit()
    return job


def consume_ids(expected_ids, group_id):
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_SERVERS,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe(["tile-jobs"])
    found = {}
    deadline = time.monotonic() + 20
    try:
        while set(found) != set(expected_ids) and time.monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is None or message.error():
                continue
            payload = json.loads(message.value())
            if payload.get("tile_id") in expected_ids:
                found[payload["tile_id"]] = message
        return found
    finally:
        consumer.close()


def test_postgres_persists_job_and_tile_state(session_factory):
    with session_factory() as db:
        job = make_job(db)
        job_id, tile_id = job.id, job.tiles[0].id
        job.status = JobStatus.MERGING
        job.tiles[0].status = TileStatus.COMPLETED
        db.commit()
    with session_factory() as restarted:
        assert restarted.get(ImageJob, job_id).status == JobStatus.MERGING
        assert restarted.get(TileTask, tile_id).status == TileStatus.COMPLETED


def test_kafka_publishes_one_keyed_message_per_tile(session_factory, monkeypatch):
    with session_factory() as db:
        tiles = make_job(db, 4).tiles
        expected_ids = {tile.id for tile in tiles}
        monkeypatch.setattr("messaging.producer.settings.kafka_bootstrap_servers", KAFKA_SERVERS)
        monkeypatch.setattr("messaging.producer.settings.kafka_tile_topic", "tile-jobs")
        _producer.cache_clear()
        publish_tile_jobs(tiles)
    messages = consume_ids(expected_ids, f"integration-{uuid.uuid4()}")
    assert set(messages) == expected_ids
    assert {message.key().decode() for message in messages.values()} == expected_ids
    _producer.cache_clear()


def test_manual_commit_occurs_after_postgres_state_is_durable(session_factory, tmp_path, monkeypatch):
    with session_factory() as db:
        job = make_job(db)
        tile = job.tiles[0]
        source = tmp_path / tile.input_path
        source.parent.mkdir(parents=True)
        (tmp_path / "jobs" / job.id / "output").mkdir(parents=True)
        Image.new("RGB", (8, 8), "red").save(source)
        tile_id = tile.id

    monkeypatch.setattr("worker.consumer.SessionLocal", session_factory)
    monkeypatch.setattr("worker.consumer.settings.worker_id", "integration-worker")
    monkeypatch.setattr("app.config.settings.storage_root", tmp_path)

    class Message:
        def value(self):
            return json.dumps({"tile_id": tile_id}).encode()

        def partition(self):
            return 0

    class CheckingConsumer:
        committed = False

        def commit(self, message, asynchronous):
            with session_factory() as db:
                assert db.get(TileTask, tile_id).status == TileStatus.COMPLETED
            assert asynchronous is False
            self.committed = True

    consumer = CheckingConsumer()
    assert handle_message(consumer, Message()) == TileStatus.COMPLETED
    assert consumer.committed


def test_three_consumers_share_all_topic_partitions():
    group_id = f"assignment-{uuid.uuid4()}"
    consumers = [
        Consumer({"bootstrap.servers": KAFKA_SERVERS, "group.id": group_id, "auto.offset.reset": "latest"})
        for _ in range(3)
    ]
    for consumer in consumers:
        consumer.subscribe(["tile-jobs"])
    deadline = time.monotonic() + 20
    assignments = []
    try:
        while time.monotonic() < deadline:
            for consumer in consumers:
                consumer.poll(0.2)
            assignments = [consumer.assignment() for consumer in consumers]
            if all(assignments) and {p.partition for assigned in assignments for p in assigned} == {0, 1, 2}:
                break
        assert all(len(assigned) == 1 for assigned in assignments)
        assert {p.partition for assigned in assignments for p in assigned} == {0, 1, 2}
    finally:
        for consumer in consumers:
            consumer.close()


def test_aggregator_reconstructs_and_propagates_permanent_failure(session_factory, tmp_path, monkeypatch):
    monkeypatch.setattr("app.config.settings.storage_root", tmp_path)
    with session_factory() as db:
        complete = make_job(db, 2)
        for index, tile in enumerate(complete.tiles):
            tile.status = TileStatus.COMPLETED
            tile.output_path = f"jobs/{complete.id}/output/{index}.png"
            output = tmp_path / tile.output_path
            output.parent.mkdir(parents=True, exist_ok=True)
            Image.new("L", (8, 8), 40 + index).save(output)
        failed = make_job(db)
        failed.tiles[0].status = TileStatus.FAILED
        db.commit()
        assert aggregate_jobs(db) == 1
        assert complete.status == JobStatus.COMPLETED
        assert failed.status == JobStatus.FAILED
        with Image.open(tmp_path / complete.final_path) as result:
            expected = Image.new("L", (16, 8))
            expected.paste(Image.new("L", (8, 8), 40), (0, 0))
            expected.paste(Image.new("L", (8, 8), 41), (8, 0))
            assert ImageChops.difference(result, expected).getbbox() is None
