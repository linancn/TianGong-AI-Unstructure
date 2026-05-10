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
    txt_name: str | None
    jsonl_sha256: str
    pkl_sha256: str
    txt_sha256: str | None
    jsonl_size_bytes: int
    pkl_size_bytes: int
    txt_size_bytes: int | None
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
    txt_path: Path | None,
    parser_profile: str,
    parser_version: str,
    embedding: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], str]:
    jsonl_name = jsonl_path.name
    pkl_name = pkl_path.name
    artifacts = {
        "chunks_jsonl": jsonl_name,
        "chunks_pkl": pkl_name,
    }
    sha256 = {
        "chunks_jsonl": file_sha256(jsonl_path),
        "chunks_pkl": file_sha256(pkl_path),
    }
    size_bytes = {
        "chunks_jsonl": jsonl_path.stat().st_size,
        "chunks_pkl": pkl_path.stat().st_size,
    }
    if txt_path is not None:
        artifacts["full_text_txt"] = txt_path.name
        sha256["full_text_txt"] = file_sha256(txt_path)
        size_bytes["full_text_txt"] = txt_path.stat().st_size

    manifest = {
        "document_id": snapshot.document_id,
        "document_version": snapshot.document_version,
        "artifact_uuid": artifact_uuid,
        "collection_id": snapshot.primary_collection_id,
        "collection_path": snapshot.collection_path,
        "collection_storage_path": snapshot.collection_storage_path,
        "processed_storage_path": snapshot.processed_storage_path,
        "chunk_count": chunk_count,
        "artifacts": artifacts,
        "sha256": sha256,
        "size_bytes": size_bytes,
        "created_at": datetime.now(UTC).isoformat(),
        "producer": "kb-parse-worker",
        "parser_profile": parser_profile,
        "parser_version": parser_version,
    }
    if embedding is not None:
        manifest["embedding"] = embedding
    manifest_bytes = json.dumps(manifest, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return manifest, hashlib.sha256(manifest_bytes).hexdigest()


def write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def load_artifact_info(manifest_path: Path, manifest_hash: str | None = None) -> ArtifactInfo:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    artifacts = manifest["artifacts"]
    sha256 = manifest["sha256"]
    size_bytes = manifest["size_bytes"]
    return ArtifactInfo(
        artifact_uuid=str(manifest["artifact_uuid"]),
        chunk_count=int(manifest["chunk_count"]),
        jsonl_name=str(artifacts["chunks_jsonl"]),
        pkl_name=str(artifacts["chunks_pkl"]),
        txt_name=str(artifacts["full_text_txt"]) if artifacts.get("full_text_txt") else None,
        jsonl_sha256=str(sha256["chunks_jsonl"]),
        pkl_sha256=str(sha256["chunks_pkl"]),
        txt_sha256=str(sha256["full_text_txt"]) if sha256.get("full_text_txt") else None,
        jsonl_size_bytes=int(size_bytes["chunks_jsonl"]),
        pkl_size_bytes=int(size_bytes["chunks_pkl"]),
        txt_size_bytes=int(size_bytes["full_text_txt"]) if size_bytes.get("full_text_txt") is not None else None,
        manifest_hash=manifest_hash or file_sha256(manifest_path),
        manifest=manifest,
    )
