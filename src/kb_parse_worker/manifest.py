"""Manifest creation and validation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .snapshot import ParseSnapshot


@dataclass(frozen=True)
class ArtifactInfo:
    artifact_uuid: str
    chunk_count: int
    jsonl_name: str
    pkl_name: str
    jsonl_sha256: str
    pkl_sha256: str
    jsonl_size_bytes: int
    pkl_size_bytes: int
    manifest_hash: str
    manifest: dict[str, Any]


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def build_manifest(
    snapshot: ParseSnapshot,
    artifact_uuid: str,
    chunk_count: int,
    jsonl_path: Path,
    pkl_path: Path,
    parser_profile: str,
    parser_version: str,
) -> tuple[dict[str, Any], str]:
    jsonl_name = jsonl_path.name
    pkl_name = pkl_path.name
    manifest = {
        "document_id": snapshot.document_id,
        "document_version": snapshot.document_version,
        "artifact_uuid": artifact_uuid,
        "collection_id": snapshot.primary_collection_id,
        "collection_path": snapshot.collection_path,
        "collection_storage_path": snapshot.collection_storage_path,
        "processed_storage_path": snapshot.processed_storage_path,
        "chunk_count": chunk_count,
        "artifacts": {
            "chunks_jsonl": jsonl_name,
            "chunks_pkl": pkl_name,
        },
        "sha256": {
            "chunks_jsonl": file_sha256(jsonl_path),
            "chunks_pkl": file_sha256(pkl_path),
        },
        "size_bytes": {
            "chunks_jsonl": jsonl_path.stat().st_size,
            "chunks_pkl": pkl_path.stat().st_size,
        },
        "created_at": datetime.now(UTC).isoformat(),
        "producer": "kb-parse-worker",
        "parser_profile": parser_profile,
        "parser_version": parser_version,
    }
    manifest_bytes = json.dumps(manifest, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return manifest, hashlib.sha256(manifest_bytes).hexdigest()


def write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
