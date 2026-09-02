import json
import logging

from confluent_kafka import Consumer, KafkaError

from app.config import settings
from app.db.models import TileStatus
from app.db.session import SessionLocal
from worker.processor import process_tile

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def log_assignment(_consumer, partitions) -> None:
    logger.info(
        "event=partitions_assigned worker_id=%s partitions=%s",
        settings.worker_id,
        ",".join(str(partition.partition) for partition in partitions),
    )


def handle_message(consumer: Consumer, message) -> TileStatus:
    payload = json.loads(message.value())
    with SessionLocal() as db:
        status = process_tile(db, payload["tile_id"], settings.worker_id)
    consumer.commit(message=message, asynchronous=False)
    logger.info(
        "event=tile_processed worker_id=%s partition=%s tile_id=%s status=%s",
        settings.worker_id,
        message.partition(),
        payload["tile_id"],
        status.value,
    )
    return status


def run() -> None:
    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": settings.kafka_consumer_group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([settings.kafka_tile_topic], on_assign=log_assignment)
    logger.info("event=worker_started worker_id=%s", settings.worker_id)
    try:
        while True:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Kafka consumer error: %s", message.error())
                continue
            handle_message(consumer, message)
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
