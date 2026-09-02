import argparse
import hashlib
import io
import json
import time
from pathlib import Path

import httpx
from PIL import Image, ImageChops


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark one image-processing job.")
    parser.add_argument("image", type=Path)
    parser.add_argument("--workers", type=int, required=True, choices=(1, 3))
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    args = parser.parse_args()

    content = args.image.read_bytes()
    with Image.open(args.image) as source:
        width, height = source.size
        reference = source.convert("L")

    started = time.perf_counter()
    with httpx.Client(timeout=120) as client:
        response = client.post(
            f"{args.url}/jobs",
            files={"upload": (args.image.name, content, "image/png" if args.image.suffix.lower() == ".png" else "image/jpeg")},
            follow_redirects=False,
        )
        if response.status_code != 303:
            raise RuntimeError(f"Upload failed: {response.status_code} {response.text}")
        job_id = response.headers["location"].split("/")[2]
        while True:
            status = client.get(f"{args.url}/jobs/{job_id}").json()
            if status["status"] in {"COMPLETED", "FAILED"}:
                break
            time.sleep(0.1)
        runtime = time.perf_counter() - started
        if status["status"] != "COMPLETED":
            raise RuntimeError(f"Job failed: {status}")
        result = Image.open(io.BytesIO(client.get(f"{args.url}/jobs/{job_id}/download").content))

    print(json.dumps({
        "workers": args.workers,
        "image_sha256": hashlib.sha256(content).hexdigest(),
        "width": width,
        "height": height,
        "megapixels": round(width * height / 1_000_000, 3),
        "tile_count": status["total_tiles"],
        "runtime_seconds": round(runtime, 3),
        "tiles_per_second": round(status["total_tiles"] / runtime, 3),
        "output_matches_reference": ImageChops.difference(result, reference).getbbox() is None,
    }))


if __name__ == "__main__":
    main()
