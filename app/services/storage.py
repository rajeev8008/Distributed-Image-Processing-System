from pathlib import Path

from app.config import settings


def storage_path(relative_path: str) -> Path:
    root = settings.storage_root.resolve()
    path = (root / relative_path).resolve()
    if not path.is_relative_to(root):
        raise ValueError("Invalid storage path")
    return path


def prepare_job_directories(job_id: str) -> None:
    for directory in ("originals", f"jobs/{job_id}/input", f"jobs/{job_id}/output", "final"):
        storage_path(directory).mkdir(parents=True, exist_ok=True)
