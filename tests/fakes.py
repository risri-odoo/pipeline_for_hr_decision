"""In-memory stand-in for job.Lake (same primitives, no network)."""

from __future__ import annotations

import hashlib

import job


class MemoryLake(job.Lake):
    def __init__(self, bucket: str = "test-lake"):
        self.bucket = bucket
        self.region = "us-east-1"
        self.objects: dict[str, bytes] = {}
        self.calls: list[tuple[str, str]] = []

    def ensure_bucket(self) -> None:
        pass

    def put(self, key: str, data: bytes) -> None:
        self.calls.append(("put", key))
        self.objects[key] = bytes(data)

    def get(self, key: str) -> bytes:
        if key not in self.objects:
            raise KeyError(key)
        return self.objects[key]

    def copy(self, src: str, dst: str) -> None:
        self.calls.append(("copy", dst))
        self.objects[dst] = self.get(src)

    def size(self, key: str):
        return len(self.objects[key]) if key in self.objects else None

    def list(self, prefix: str) -> list[str]:
        return sorted(k for k in self.objects if k.startswith(prefix))

    def fingerprints(self, prefix: str) -> dict[str, str]:
        return {k: hashlib.md5(self.objects[k]).hexdigest() for k in self.list(prefix)}

    def delete_prefix(self, prefix: str) -> int:
        keys = self.list(prefix)
        for k in keys:
            del self.objects[k]
        return len(keys)


def csv_exporter(data_dir):
    """Exporter that serves the generated CSVs as if Postgres COPY produced them."""

    def export(settings, table):
        return (data_dir / f"{table}.csv").read_bytes()

    return export
