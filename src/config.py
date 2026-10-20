"""Environment-backed configuration for the analyst variant 10 pipeline."""
from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import urlparse


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class Settings:
    endpoint_url: str = os.getenv("S3_ENDPOINT_URL", "http://rustfs:9000")
    access_key: str = os.getenv("S3_ACCESS_KEY_ID", os.getenv("RUSTFS_ACCESS_KEY", ""))
    secret_key: str = os.getenv("S3_SECRET_ACCESS_KEY", os.getenv("RUSTFS_SECRET_KEY", ""))
    region: str = os.getenv("S3_REGION", "us-east-1")
    bucket: str = os.getenv("S3_BUCKET", "hh-lake")
    object_key: str = os.getenv("S3_OBJECT_KEY", "raw/analyst/vacancies.jsonl")
    hh_user_agent: str = os.getenv("HH_USER_AGENT", "AnalystVariant10/1.0 (educational project)")
    hh_delay: float = _float_env("HH_REQUEST_DELAY_SECONDS", 0.12)
    chunk_sizes: tuple[int, ...] = tuple(
        int(part.strip()) for part in os.getenv("CHUNK_SIZES", "100,500,1000").split(",")
        if part.strip().isdigit() and int(part.strip()) > 0
    ) or (100, 500, 1000)
    profile_attempts: int = max(3, _int_env("PROFILE_ATTEMPTS", 3))
    min_combo_support: int = max(2, _int_env("MIN_COMBO_SUPPORT", 5))
    artifacts_dir: str = os.getenv("ARTIFACTS_DIR", "artifacts")

    @property
    def duckdb_endpoint(self) -> str:
        parsed = urlparse(self.endpoint_url)
        if not parsed.netloc:
            raise ValueError(f"S3_ENDPOINT_URL must include a scheme and host: {self.endpoint_url!r}")
        return parsed.netloc

    @property
    def duckdb_use_ssl(self) -> bool:
        return urlparse(self.endpoint_url).scheme.lower() == "https"

    def validate(self) -> None:
        missing = []
        if not self.access_key:
            missing.append("S3_ACCESS_KEY_ID / RUSTFS_ACCESS_KEY")
        if not self.secret_key:
            missing.append("S3_SECRET_ACCESS_KEY / RUSTFS_SECRET_KEY")
        if missing:
            raise ValueError("Не заданы параметры доступа к RustFS: " + ", ".join(missing))
        if not self.chunk_sizes:
            raise ValueError("CHUNK_SIZES должен содержать хотя бы один положительный размер чанка")


settings = Settings()
