import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Settings:
    database_url: str = os.getenv("DATABASE_URL", "sqlite:///./image_processing.db")
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_tile_topic: str = os.getenv("KAFKA_TILE_TOPIC", "tile-jobs")
    kafka_consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "tile-workers")
    storage_root: Path = Path(os.getenv("STORAGE_ROOT", "storage"))
    tile_size: int = int(os.getenv("TILE_SIZE", "512"))
    max_upload_size_mb: int = int(os.getenv("MAX_UPLOAD_SIZE_MB", "100"))
    max_processing_attempts: int = int(os.getenv("MAX_PROCESSING_ATTEMPTS", "3"))
    aggregator_interval_seconds: float = float(os.getenv("AGGREGATOR_INTERVAL_SECONDS", "2"))
    worker_id: str = os.getenv("WORKER_ID", os.getenv("HOSTNAME", "local-worker"))


settings = Settings()
