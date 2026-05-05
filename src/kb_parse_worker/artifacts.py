"""Write parser results to processed artifacts."""

from __future__ import annotations

import json
import os
import pickle
import shutil
import uuid
from pathlib import Path
from typing import Any

from .manifest import ArtifactInfo, build_manifest, file_sha256, write_manifest
from .snapshot import ParseSnapshot


def _safe_replace_dir(src: Path, dst: Path) -> None:
    backup = dst.with_name(f"{dst.name}.previous")
    if backup.exists():
        shutil.rmtree(backup)
    if dst.exists():
        dst.rename(backup)
    src.rename(dst)
    if backup.exists():
        shutil.rmtree(backup)


def write_processed_artifacts(
    result: list[Any],
    snapshot: ParseSnapshot,
    nas_processed_root: Path,
    parser_profile: str,
    parser_version: str,
) -> tuple[Path, ArtifactInfo]:
    if not result:
        raise ValueError("EMPTY_RESULT")

    artifact_uuid = str(uuid.uuid4())
    collection_root = nas_processed_root / snapshot.processed_storage_path
    tmp_dir = collection_root / ".tmp" / f"{snapshot.job_id}-{artifact_uuid}"
    final_dir = collection_root / snapshot.document_id
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=False)

    jsonl_path = tmp_dir / f"{artifact_uuid}.jsonl"
    pkl_path = tmp_dir / f"{artifact_uuid}.pkl"
    with jsonl_path.open("w", encoding="utf-8") as handle:
        for item in result:
            handle.write(json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n")
    with pkl_path.open("wb") as handle:
        pickle.dump(result, handle)

    if sum(1 for _ in jsonl_path.open("r", encoding="utf-8")) != len(result):
        raise RuntimeError("ARTIFACT_VALIDATE_FAILED: jsonl row count mismatch")
    with pkl_path.open("rb") as handle:
        pickle.load(handle)
    if jsonl_path.stat().st_size <= 0 or pkl_path.stat().st_size <= 0:
        raise RuntimeError("ARTIFACT_VALIDATE_FAILED: empty artifact file")

    manifest, _ = build_manifest(
        snapshot=snapshot,
        artifact_uuid=artifact_uuid,
        chunk_count=len(result),
        jsonl_path=jsonl_path,
        pkl_path=pkl_path,
        parser_profile=parser_profile,
        parser_version=parser_version,
    )
    write_manifest(tmp_dir / "manifest.tmp.json", manifest)
    os.replace(tmp_dir / "manifest.tmp.json", tmp_dir / "manifest.json")
    manifest_hash = file_sha256(tmp_dir / "manifest.json")

    _safe_replace_dir(tmp_dir, final_dir)
    info = ArtifactInfo(
        artifact_uuid=artifact_uuid,
        chunk_count=len(result),
        jsonl_name=jsonl_path.name,
        pkl_name=pkl_path.name,
        jsonl_sha256=manifest["sha256"]["chunks_jsonl"],
        pkl_sha256=manifest["sha256"]["chunks_pkl"],
        jsonl_size_bytes=manifest["size_bytes"]["chunks_jsonl"],
        pkl_size_bytes=manifest["size_bytes"]["chunks_pkl"],
        manifest_hash=manifest_hash,
        manifest=manifest,
    )
    return final_dir, info
