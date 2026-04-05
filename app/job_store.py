from __future__ import annotations

import json
from pathlib import Path

from app.models import SearchJobStatus


class FileJobStore:
    def __init__(self, directory: str):
        self.root = Path(directory)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, job_id: str) -> Path:
        return self.root / f"{job_id}.json"

    def save(self, status: SearchJobStatus) -> None:
        self._path(status.job_id).write_text(
            json.dumps(status.model_dump(), indent=2, ensure_ascii=True),
            encoding="utf-8",
        )

    def load(self, job_id: str) -> SearchJobStatus | None:
        path = self._path(job_id)
        if not path.exists():
            return None
        return SearchJobStatus.model_validate(json.loads(path.read_text(encoding="utf-8")))

    def list(self) -> list[SearchJobStatus]:
        statuses: list[SearchJobStatus] = []
        for path in sorted(self.root.glob("*.json"), reverse=True):
            try:
                statuses.append(
                    SearchJobStatus.model_validate(json.loads(path.read_text(encoding="utf-8")))
                )
            except Exception:
                continue
        return statuses

    def delete(self, job_id: str) -> None:
        self._path(job_id).unlink(missing_ok=True)
