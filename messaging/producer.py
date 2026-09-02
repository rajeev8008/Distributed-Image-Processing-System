import json
from functools import lru_cache
from typing import Iterable

from confluent_kafka import Producer

from app.config import settings
from app.db.models import TileTask


@lru_cache
def _producer() -> Producer:
    return Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})


def publish_tile_jobs(tiles: Iterable[TileTask]) -> None:
    producer = _producer()
    errors = []

    def delivered(error, _message) -> None:
        if error:
            errors.append(error)

    for tile in tiles:
        message = {
            "job_id": tile.job_id,
            "tile_id": tile.id,
            "tile_index": tile.tile_index,
            "input_path": tile.input_path,
        }
        producer.produce(
            settings.kafka_tile_topic,
            key=tile.id,
            value=json.dumps(message, separators=(",", ":")),
            on_delivery=delivered,
        )
    remaining = producer.flush(10)
    if remaining or errors:
        raise RuntimeError(f"Kafka did not deliver {remaining or len(errors)} tile message(s)")

