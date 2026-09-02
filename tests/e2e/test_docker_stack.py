import io
import os
import socket
import subprocess
import time
import uuid

import httpx
from PIL import Image, ImageChops


def compose(project, environment, *arguments, check=True):
    return subprocess.run(
        ["docker", "compose", "-p", project, *arguments],
        cwd=os.getcwd(),
        env=environment,
        check=check,
        capture_output=True,
        text=True,
    )


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def deterministic_image():
    image = Image.new("RGB", (1025, 770))
    image.putdata(
        [(x % 256, y % 256, (x * 3 + y * 5) % 256) for y in range(image.height) for x in range(image.width)]
    )
    buffer = io.BytesIO()
    image.save(buffer, "PNG")
    return image, buffer.getvalue()


def wait_for_zero_lag(project, environment):
    deadline = time.monotonic() + 30
    last_output = "consumer group was not available"
    while time.monotonic() < deadline:
        result = compose(
            project,
            environment,
            "exec",
            "-T",
            "kafka",
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server",
            "kafka:9092",
            "--describe",
            "--group",
            "tile-workers",
            check=False,
        )
        last_output = result.stdout + result.stderr
        rows = [line.split() for line in result.stdout.splitlines() if line.startswith("tile-workers")]
        if rows and all(row[5] == "0" or (row[4] == "0" and row[5] == "-") for row in rows):
            return
        time.sleep(0.5)
    raise AssertionError(f"Kafka lag did not reach zero:\n{last_output}")


def test_docker_compose_pipeline_is_pixel_exact():
    project = f"dips-e2e-{uuid.uuid4().hex[:8]}"
    environment = os.environ.copy()
    environment["POSTGRES_PASSWORD"] = uuid.uuid4().hex
    environment["API_PORT"] = str(free_port())
    base_url = f"http://127.0.0.1:{environment['API_PORT']}"
    try:
        compose(project, environment, "up", "-d", "--build", "--wait", "--scale", "worker=3")
        workers = compose(project, environment, "ps", "-q", "worker").stdout.splitlines()
        assert len(set(workers)) == 3

        source, content = deterministic_image()
        with httpx.Client(base_url=base_url, timeout=10) as client:
            response = client.post("/jobs", files={"upload": ("e2e.png", content, "image/png")})
            assert response.status_code == 303
            job_id = response.headers["location"].split("/")[2]
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                status = client.get(f"/jobs/{job_id}").json()
                if status["status"] in {"COMPLETED", "FAILED"}:
                    break
                time.sleep(0.5)
            assert status["status"] == "COMPLETED", status
            downloaded = client.get(f"/jobs/{job_id}/download")
            downloaded.raise_for_status()

        with Image.open(io.BytesIO(downloaded.content)) as result:
            assert result.size == source.size
            assert ImageChops.difference(result, source.convert("L")).getbbox() is None
        wait_for_zero_lag(project, environment)

        logs = compose(project, environment, "logs", "worker").stdout
        worker_ids = {
            token.removeprefix("worker_id=")
            for line in logs.splitlines()
            for token in line.split()
            if token.startswith("worker_id=")
        }
        assert len(worker_ids) == 3
    finally:
        compose(project, environment, "down", "-v", "--remove-orphans", check=False)
