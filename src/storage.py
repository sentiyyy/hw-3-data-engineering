"""S3-compatible object storage access for RustFS."""
from __future__ import annotations

import json
import logging
import time
from typing import Iterable, Iterator

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError

from .config import settings

log = logging.getLogger(__name__)


def s3_client():
    settings.validate()
    return boto3.client(
        "s3",
        endpoint_url=settings.endpoint_url,
        aws_access_key_id=settings.access_key,
        aws_secret_access_key=settings.secret_key,
        region_name=settings.region,
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 4, "mode": "standard"}),
    )


def ensure_bucket(client=None) -> None:
    client = client or s3_client()
    for attempt in range(1, 16):
        try:
            existing = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
            if settings.bucket not in existing:
                client.create_bucket(Bucket=settings.bucket)
                log.info("Создан бакет %s", settings.bucket)
            else:
                log.info("Бакет %s уже существует", settings.bucket)
            return
        except (EndpointConnectionError, ConnectionError) as exc:
            if attempt == 15:
                raise RuntimeError(f"Не удалось подключиться к RustFS {settings.endpoint_url}: {exc}") from exc
            time.sleep(min(attempt, 4))
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                return
            if attempt == 15:
                raise
            time.sleep(min(attempt, 4))


def encode_jsonl(rows: Iterable[dict]) -> bytes:
    return ("\n".join(json.dumps(row, ensure_ascii=False, separators=(",", ":")) for row in rows) + "\n").encode("utf-8")


def upload_jsonl(rows: list[dict], key: str | None = None, client=None) -> str:
    client = client or s3_client()
    ensure_bucket(client)
    key = key or settings.object_key
    body = encode_jsonl(rows)
    client.put_object(Bucket=settings.bucket, Key=key, Body=body, ContentType="application/x-ndjson; charset=utf-8")
    log.info("Загружен объект s3://%s/%s (%d записей, %.2f KiB)", settings.bucket, key, len(rows), len(body) / 1024)
    return key


def iter_jsonl_from_s3(key: str | None = None, client=None) -> Iterator[dict]:
    """Stream NDJSON from S3; the entire object is never loaded by this iterator."""
    client = client or s3_client()
    key = key or settings.object_key
    response = client.get_object(Bucket=settings.bucket, Key=key)
    body = response["Body"]
    try:
        for line_no, line in enumerate(body.iter_lines(), 1):
            if not line:
                continue
            try:
                yield json.loads(line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError(f"Повреждён JSONL в объекте {key}, строка {line_no}: {exc}") from exc
    finally:
        body.close()


def load_all_records(key: str | None = None, client=None) -> list[dict]:
    return list(iter_jsonl_from_s3(key=key, client=client))


def iter_chunks(records: Iterable[dict], chunk_size: int) -> Iterator[list[dict]]:
    batch: list[dict] = []
    for record in records:
        batch.append(record)
        if len(batch) >= chunk_size:
            yield batch
            batch = []
    if batch:
        yield batch


def upload_json(key: str, payload: dict | list, client=None) -> None:
    client = client or s3_client()
    ensure_bucket(client)
    body = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
    client.put_object(Bucket=settings.bucket, Key=key, Body=body, ContentType="application/json; charset=utf-8")
    log.info("Сохранён объект s3://%s/%s", settings.bucket, key)


def upload_file(key: str, path: str, content_type: str = "application/pdf", client=None) -> None:
    client = client or s3_client()
    ensure_bucket(client)
    with open(path, "rb") as f:
        client.put_object(Bucket=settings.bucket, Key=key, Body=f, ContentType=content_type)
    log.info("Сохранён файл s3://%s/%s", settings.bucket, key)


def list_objects(prefix: str = "", client=None) -> list[dict]:
    client = client or s3_client()
    ensure_bucket(client)
    items: list[dict] = []
    token = None
    while True:
        args = {"Bucket": settings.bucket, "Prefix": prefix}
        if token:
            args["ContinuationToken"] = token
        response = client.list_objects_v2(**args)
        for item in response.get("Contents", []):
            items.append({"key": item["Key"], "size": item["Size"], "last_modified": item["LastModified"].isoformat()})
        if not response.get("IsTruncated"):
            break
        token = response.get("NextContinuationToken")
    return items
