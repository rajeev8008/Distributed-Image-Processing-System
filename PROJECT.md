# Distributed Image Processing System

## 1. Project summary

Build a tile-based image-processing system that accepts one high-resolution image, splits it into fixed-size tiles, distributes the tiles through Apache Kafka, processes them concurrently using Docker worker containers, and reconstructs the processed tiles into one final image.

The first version supports one operation: grayscale conversion. The goal is to demonstrate master-worker architecture, data partitioning, Kafka consumer groups, parallel execution, persistent progress tracking, idempotency, and result aggregation without adding unnecessary infrastructure.

## 2. Problem

Processing a very large image as one unit:

- makes the user wait for a long HTTP request;
- uses only one processing path;
- makes it difficult to retry only the failed portion;
- cannot easily distribute work between workers.

The system divides the image into independent tiles. Kafka distributes those tile jobs between workers, and an aggregator rebuilds the final image after every tile completes.

## 3. Final scope

The application must:

- Accept one JPEG or PNG image per job.
- Validate image type, size, and decodability.
- Split the image into tiles of at most 512 x 512 pixels.
- Correctly handle smaller tiles at the right and bottom edges.
- Save one database record per image job and per tile.
- Publish one Kafka message per tile.
- Run three identical worker containers in one Kafka consumer group.
- Convert every tile to grayscale using Pillow.
- Save processed tiles to a shared Docker volume.
- Track job and tile progress in PostgreSQL.
- Merge tiles using their original x and y coordinates.
- Allow the user to poll job progress and download the final image.
- Retry a tile up to three times.
- Safely handle Kafka redelivery without duplicating work.
- Benchmark one worker against three workers.

## 4. Non-goals

Do not add:

- Kubernetes
- Virtual machines or separate physical nodes
- MinIO, S3, or cloud deployment
- Redis or Celery
- Multiple image-processing filters
- Overlapping tiles
- Video processing
- WebSockets
- Authentication
- React or Next.js
- Microservices
- Prometheus or Grafana
- A separate Kafka result topic or dead-letter topic

The final system runs as multiple Docker containers on one machine.

## 5. User workflow

1. The user selects one high-resolution image.
2. FastAPI creates an image job and stores the original image.
3. The coordinator divides the image into 512 x 512 tiles.
4. Each tile and its coordinates are stored.
5. The coordinator publishes one Kafka message per tile.
6. Three workers consume tile jobs in parallel.
7. Each worker converts its tile to grayscale and updates PostgreSQL.
8. The aggregator detects when all tiles are complete.
9. The aggregator places every tile at its original coordinates.
10. The user downloads the reconstructed grayscale image.

## 6. Components

### FastAPI and coordinator

- Accept the uploaded image.
- Create the job and tile records.
- Split and store the tiles.
- Publish tile messages to Kafka.
- Expose job status and download endpoints.
- Return a job ID without waiting for processing to finish.

### Kafka

- Topic: `tile-jobs`
- Partitions: 3
- Consumer group: `tile-workers`
- Message key: `tile_id`

Using `tile_id` as the key distributes tiles from the same image across partitions. Do not use `job_id` as the key because that could send all tiles for one image to one partition.

### Worker containers

- Run identical Python code from the same Docker image.
- Consume from the `tile-jobs` topic.
- Process one tile at a time.
- Use unique worker IDs in logs.
- Persist the outcome before committing the Kafka offset.

### PostgreSQL

- Store job metadata and status.
- Store tile coordinates, paths, status, attempts, and worker ID.
- Act as the source of truth for aggregation and progress.

### Aggregator

- Run as one separate container.
- Poll PostgreSQL for processing jobs.
- Merge a job only when every tile is completed.
- Mark the job failed when any tile permanently fails.
- Save the final image to shared storage.

### Shared Docker volume

```text
storage/
  originals/
  jobs/{job_id}/input/
  jobs/{job_id}/output/
  final/
```

The API, workers, and aggregator mount the same volume.

## 7. Data model

### ImageJob

- `id`: UUID primary key
- `original_filename`: string
- `original_path`: string
- `final_path`: nullable string
- `width`: integer
- `height`: integer
- `tile_size`: integer, default 512
- `total_tiles`: integer
- `status`: `SPLITTING | PROCESSING | MERGING | COMPLETED | FAILED`
- `error_message`: nullable string
- `created_at`: timestamp
- `completed_at`: nullable timestamp

### TileTask

- `id`: UUID primary key
- `job_id`: foreign key
- `tile_index`: integer
- `x`: integer
- `y`: integer
- `width`: integer
- `height`: integer
- `input_path`: string
- `output_path`: nullable string
- `status`: `QUEUED | PROCESSING | COMPLETED | FAILED`
- `attempt_count`: integer, default 0
- `worker_id`: nullable string
- `processing_time_ms`: nullable integer
- `error_message`: nullable string
- `created_at`: timestamp
- `completed_at`: nullable timestamp

Add a unique constraint on `(job_id, tile_index)`.

## 8. Kafka message

```json
{
  "job_id": "01d2d1c1-1372-44ea-ab47-f91b1417e024",
  "tile_id": "96cf530c-e7d8-4f92-8423-3eeaeb8e00f7",
  "tile_index": 4,
  "input_path": "storage/jobs/01d2/input/4.png"
}
```

Kafka carries metadata and file paths, never image bytes.

## 9. API

- `GET /` — upload page
- `POST /jobs` — create a processing job
- `GET /jobs/{job_id}` — return job progress and tile counts
- `GET /jobs/{job_id}/view` — basic HTML progress page
- `GET /jobs/{job_id}/download` — download the final image
- `GET /health` — API and database health

Example progress response:

```json
{
  "job_id": "01d2d1c1-1372-44ea-ab47-f91b1417e024",
  "status": "PROCESSING",
  "completed_tiles": 12,
  "failed_tiles": 0,
  "total_tiles": 20,
  "progress_percent": 60
}
```

## 10. Implementation stages

### Stage 1 — Local tile correctness

- Create the FastAPI project and database models.
- Upload and validate one image.
- Split it into tiles.
- Convert tiles to grayscale sequentially.
- Reconstruct the final image.
- Test edge tiles and final dimensions.

Acceptance: tiled grayscale output matches a whole-image grayscale reference.

### Stage 2 — Kafka and one worker

- Add Kafka and PostgreSQL to Docker Compose.
- Publish one message for every tile.
- Move tile processing to one worker.
- Disable automatic Kafka offset commits.
- Persist the tile outcome before committing the offset.

Acceptance: the upload request returns a job ID before tile processing finishes.

### Stage 3 — Three worker containers and aggregation

- Containerize the worker.
- Run three workers in consumer group `tile-workers`.
- Give each worker a unique ID.
- Add the separate polling aggregator.
- Add the browser progress page.

Acceptance: logs show that different workers process tiles from different partitions, and the final image is reconstructed automatically.

### Stage 4 — Reliability and benchmarking

- Retry tile processing up to three times.
- Skip a redelivered tile that is already completed.
- Use deterministic output filenames.
- Stop one worker and verify remaining consumers receive reassigned partitions.
- Benchmark one worker and three workers using the same images and settings.
- Add tests and final README documentation.

Acceptance: worker interruption does not corrupt the final output, and benchmark results are reproducible.

## 11. Idempotency and failure rules

- Disable Kafka auto-commit.
- A worker checks the tile status before processing.
- If a tile is already `COMPLETED`, commit the message without processing again.
- Save output as `storage/jobs/{job_id}/output/{tile_index}.png`.
- Update PostgreSQL before committing the Kafka offset.
- Retry processing locally up to three times.
- After the final failure, mark the tile `FAILED` and commit the message.
- The aggregator marks the parent image job `FAILED` if any tile fails.

## 12. Benchmark

Run the same workload with:

- one worker container;
- three worker containers.

Record:

- image resolution and megapixels;
- number of tiles;
- total processing time;
- tiles processed per second;
- output correctness;
- worker distribution from logs.

Do not claim a speed improvement unless the measured results show one. If three workers are not faster, report the measured throughput and explain Docker, disk, Kafka, and CPU contention.

## 13. Definition of done

- A high-resolution image can be uploaded and processed.
- Tiles are distributed across three Kafka partitions.
- Three worker containers participate in the same consumer group.
- Edge tiles are reconstructed correctly.
- Job progress survives API restarts.
- Duplicate Kafka delivery does not duplicate completed work.
- Stopping one worker causes Kafka partition reassignment.
- The final output matches the reference operation.
- One-worker and three-worker benchmarks are documented.

## 14. Resume points

> **Distributed Image Processing System**  
> Python, FastAPI, Apache Kafka, PostgreSQL, Pillow, Docker

- Designed a Kafka-driven master-worker system that partitions high-resolution images into 512 x 512 tiles and distributes processing across parallel worker containers.
- Used Kafka topic partitioning and consumer groups to load-balance tile jobs, with PostgreSQL-backed progress tracking and idempotent task execution.
- Implemented coordinate-based result aggregation to reconstruct processed tiles into the original image dimensions without ordering dependencies.
- Benchmarked one-worker and three-worker configurations, achieving **X% lower processing time** on **Y-megapixel images**.

Replace X and Y only with real measured values. If processing time does not improve, rewrite the last point using measured tiles-per-second throughput.

## 15. Git rules

- `IDE_PROMPT.md`, `ide_prompt.md`, and `prompts.md` are local agent-instruction files and must be added to `.gitignore`.
- Never commit an IDE prompt file, even with a forced add.
- Commit messages must describe the implemented feature or fix.
- Do not mention stages, phases, or stage numbers in commit messages.
- Do not create a commit unless the current changes have been tested.

Good commit messages:

```text
feat: add image tiling and coordinate reconstruction
feat: distribute tile jobs through Kafka consumers
feat: add PostgreSQL-backed processing status
test: verify worker reassignment and idempotent delivery
```

Bad commit messages:

```text
complete phase 1
implement stage 2
phase 3 changes
```

