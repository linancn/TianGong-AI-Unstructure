"""S3 processed artifact ready checks."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass

import boto3
from botocore.exceptions import ClientError

from .manifest import ArtifactInfo
from .snapshot import ParseSnapshot


@dataclass(frozen=True)
class S3ReadyResult:
    ready: bool
    manifest_s3_key: str


def processed_manifest_key(prefix: str, snapshot: ParseSnapshot) -> str:
    clean_prefix = prefix.strip("/")
    return f"{clean_prefix}/{snapshot.processed_storage_path}/{snapshot.document_id}/manifest.json"


def _check_s3_processed_ready(
    snapshot: ParseSnapshot,
    artifact_info: ArtifactInfo,
    bucket: str,
    prefix: str,
    strict_hash: bool = False,
) -> S3ReadyResult:
    client = boto3.client("s3")
    manifest_key = processed_manifest_key(prefix, snapshot)
    client.head_object(Bucket=bucket, Key=manifest_key)
    manifest_obj = client.get_object(Bucket=bucket, Key=manifest_key)
    manifest = json.loads(manifest_obj["Body"].read().decode("utf-8"))

    expected = artifact_info.manifest
    for field in ("document_id", "document_version", "artifact_uuid", "collection_path"):
        if manifest.get(field) != expected.get(field):
            raise RuntimeError(f"S3_MANIFEST_MISMATCH: {field}")
    if manifest.get("collection_storage_path") != expected.get("collection_storage_path"):
        raise RuntimeError("S3_MANIFEST_MISMATCH: collection_storage_path")
    if manifest.get("processed_storage_path") != expected.get("processed_storage_path"):
        raise RuntimeError("S3_MANIFEST_MISMATCH: processed_storage_path")

    base = "/".join(manifest_key.split("/")[:-1])
    for artifact_key, artifact_name in manifest["artifacts"].items():
        object_key = f"{base}/{artifact_name}"
        head = client.head_object(Bucket=bucket, Key=object_key)
        expected_size = int(manifest["size_bytes"][artifact_key])
        if int(head["ContentLength"]) != expected_size:
            raise RuntimeError(f"S3_ARTIFACT_MISMATCH: {artifact_name} size")
        if strict_hash:
            body = client.get_object(Bucket=bucket, Key=object_key)["Body"]
            digest = hashlib.sha256()
            for chunk in iter(lambda: body.read(1024 * 1024), b""):
                digest.update(chunk)
            if digest.hexdigest() != manifest["sha256"][artifact_key]:
                raise RuntimeError(f"S3_ARTIFACT_MISMATCH: {artifact_name} sha256")

    return S3ReadyResult(ready=True, manifest_s3_key=manifest_key)


def wait_for_s3_processed_ready(
    snapshot: ParseSnapshot,
    artifact_info: ArtifactInfo,
    bucket: str,
    prefix: str,
    strict_hash: bool = False,
    timeout_seconds: int = 900,
    poll_interval_seconds: int = 15,
) -> S3ReadyResult:
    deadline = time.monotonic() + max(0, timeout_seconds)
    interval = max(1, poll_interval_seconds)

    while True:
        try:
            return _check_s3_processed_ready(
                snapshot,
                artifact_info,
                bucket,
                prefix,
                strict_hash,
            )
        except (ClientError, RuntimeError) as exc:
            if time.monotonic() >= deadline:
                raise RuntimeError(f"S3_NOT_READY_AFTER_TIMEOUT: {exc}") from exc
            time.sleep(min(interval, max(0.0, deadline - time.monotonic())))
