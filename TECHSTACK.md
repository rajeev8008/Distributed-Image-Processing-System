# Technology Stack

## 1. Required technologies

| Area | Technology | Purpose |
| --- | --- | --- |
| Language | Python 3.12 | API, coordinator, workers, and aggregator |
| API | FastAPI + Uvicorn | Upload, status, and download endpoints |
| Image processing | Pillow | Tile splitting, grayscale conversion, and merging |
| Database | PostgreSQL | Persistent job and tile state |
| ORM | SQLAlchemy 2.x | Typed database access |
| Migrations | Alembic | Versioned database schema |
| Messaging | Apache Kafka | Partitioned asynchronous tile distribution |
| Kafka client | confluent-kafka | Producer and consumer implementation |
| UI | Jinja2 + HTML/CSS + small JavaScript | Upload page and polling progress |
| Containers | Docker + Docker Compose | Run the complete local system |
| Tests | pytest + FastAPI TestClient | Unit, API, worker, and integration tests |

## 2. Docker Compose services

```text
api          1 container
kafka        1 container
postgres     1 container
worker       3 containers from the same image
aggregator   1 container
```

Use one worker service that can be scaled:

```bash
docker compose up --build --scale worker=3
```

Do not define a fixed `container_name` for the worker service because Compose cannot scale a service that has one fixed container name.

## 3. Kafka configuration

- Topic: `tile-jobs`
- Partitions: `3`
- Replication factor: `1` for local development
- Consumer group: `tile-workers`
- Message key: `tile_id`
- Auto offset commit: disabled

Use Kafka in KRaft mode if supported by the selected pinned image. Keep one broker because this project demonstrates worker distribution, not broker-cluster administration.

## 4. Repository structure

```text
distributed-image-processing/
  app/
    api/
      jobs.py
    db/
      models.py
      session.py
    services/
      splitter.py
      merger.py
      storage.py
    templates/
      upload.html
      job.html
    config.py
    main.py
  messaging/
    producer.py
    consumer_config.py
    topics.py
  worker/
    consumer.py
    processor.py
  aggregator/
    runner.py
  tests/
    unit/
    integration/
  storage/
  alembic/
  docker-compose.yml
  Dockerfile
  alembic.ini
  requirements.txt
  .env.example
  .gitignore
  README.md
  PROJECT.md
  ARCHITECTURE.md
  TECHSTACK.md
```

`IDE_PROMPT.md` must remain local and ignored by Git.

## 5. Environment variables

```env
DATABASE_URL=postgresql+psycopg://image_app:${POSTGRES_PASSWORD}@postgres:5432/image_processing
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TILE_TOPIC=tile-jobs
KAFKA_CONSUMER_GROUP=tile-workers
TILE_SIZE=512
MAX_UPLOAD_SIZE_MB=100
MAX_PROCESSING_ATTEMPTS=3
AGGREGATOR_INTERVAL_SECONDS=2
STORAGE_ROOT=/data
```

Each worker also receives a generated or hostname-based `WORKER_ID`.

## 6. Shared storage

Use a named Docker volume mounted as `/data` in:

- API
- workers
- aggregator

The database stores paths relative to the storage root where practical.

## 7. Development commands

Run one worker for baseline testing:

```bash
docker compose up --build --scale worker=1
```

Run three workers:

```bash
docker compose up --build --scale worker=3
```

Run tests:

```bash
pytest
```

Apply migrations:

```bash
alembic upgrade head
```

## 8. Git rules

The repository `.gitignore` must include:

```gitignore
IDE_PROMPT.md
ide_prompt.md
prompts.md
.env
storage/
__pycache__/
.pytest_cache/
```

Never commit an IDE prompt file. Commit messages must describe functionality and must not contain “phase,” “stage,” or their numbers.

## 9. Excluded technologies

Do not introduce:

- Kubernetes
- MinIO or S3
- Redis or Celery
- React
- WebSockets
- Video processing
- Multiple physical or virtual nodes
- Kafka result or dead-letter topics
- Monitoring platforms

The system should remain a clear Docker Compose project focused on Kafka and tile-based parallel processing.
