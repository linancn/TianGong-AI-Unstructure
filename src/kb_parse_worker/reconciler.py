"""Replay parse finalization from deterministic processed artifacts."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path

import psycopg2.extras

from . import control_plane, queue
from .config import WorkerConfig
from .manifest import ArtifactInfo, load_artifact_info
from .snapshot import collection_storage_path, processed_storage_path

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ParseFinalizationCandidate:
    job_id: str
    document_id: str
    document_version: int
    job_status: str
    document_status: str
    collection_path: str
    collection_storage_path: str
    processed_storage_path: str
    pgmq_queue: str | None
    pgmq_msg_id: int | None
    updated_at: str | None


def canonical_manifest_hash(manifest: dict) -> str:
    manifest_bytes = json.dumps(manifest, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(manifest_bytes).hexdigest()


def list_parse_finalization_candidates(
    conn,
    limit: int,
    document_id: str | None = None,
) -> list[ParseFinalizationCandidate]:
    if limit <= 0:
        raise ValueError("limit must be positive")

    filters = [
        "j.stage = 'parse'::public.kb_job_stage",
        "j.document_version = d.document_version",
        "d.deleted_at is null",
        "d.processed_manifest_local_uri is null",
        "d.processed_artifact_uuid is null",
        """(
          j.status in ('failed'::public.kb_job_status, 'dead'::public.kb_job_status)
          or (
            j.status = 'running'::public.kb_job_status
            and (j.locked_until is null or j.locked_until < now())
          )
        )""",
    ]
    params: list[object] = []
    if document_id is not None:
        filters.append("d.id = %s")
        params.append(document_id)
    params.append(limit)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"""
            select
              j.id as job_id,
              j.document_id,
              j.document_version,
              j.status as job_status,
              j.pgmq_queue,
              j.pgmq_msg_id,
              j.updated_at,
              d.status as document_status,
              c.path as collection_path
            from public.kb_jobs j
            join public.kb_documents d on d.id = j.document_id
            join public.kb_collections c on c.id = d.primary_collection_id
            where {" and ".join(filters)}
            order by j.updated_at asc, j.created_at asc
            limit %s
            """,
            params,
        )
        rows = cur.fetchall()
    conn.commit()

    candidates: list[ParseFinalizationCandidate] = []
    for row in rows:
        storage_path = collection_storage_path(str(row["collection_path"]))
        candidates.append(
            ParseFinalizationCandidate(
                job_id=str(row["job_id"]),
                document_id=str(row["document_id"]),
                document_version=int(row["document_version"]),
                job_status=str(row["job_status"]),
                document_status=str(row["document_status"]),
                collection_path=str(row["collection_path"]),
                collection_storage_path=storage_path,
                processed_storage_path=processed_storage_path(storage_path),
                pgmq_queue=str(row["pgmq_queue"]) if row["pgmq_queue"] else None,
                pgmq_msg_id=int(row["pgmq_msg_id"]) if row["pgmq_msg_id"] is not None else None,
                updated_at=row["updated_at"].isoformat() if row["updated_at"] is not None else None,
            )
        )
    return candidates


def manifest_path_for_candidate(config: WorkerConfig, candidate: ParseFinalizationCandidate) -> Path:
    return (
        config.nas_processed_root
        / candidate.processed_storage_path
        / candidate.document_id
        / "manifest.json"
    )


def processed_manifest_s3_key(config: WorkerConfig, candidate: ParseFinalizationCandidate) -> str:
    clean_prefix = config.s3_processed_prefix.strip("/")
    return f"{clean_prefix}/{candidate.processed_storage_path}/{candidate.document_id}/manifest.json"


def validate_manifest(candidate: ParseFinalizationCandidate, artifact_info: ArtifactInfo) -> None:
    manifest = artifact_info.manifest
    expected = {
        "document_id": candidate.document_id,
        "document_version": candidate.document_version,
        "collection_path": candidate.collection_path,
        "collection_storage_path": candidate.collection_storage_path,
        "processed_storage_path": candidate.processed_storage_path,
    }
    for key, value in expected.items():
        if manifest.get(key) != value:
            raise RuntimeError(f"MANIFEST_MISMATCH: {key}")


def validate_local_artifact_files(manifest_path: Path, artifact_info: ArtifactInfo) -> None:
    manifest = artifact_info.manifest
    base_dir = manifest_path.parent
    for artifact_key, artifact_name in manifest["artifacts"].items():
        artifact_path = base_dir / str(artifact_name)
        if not artifact_path.exists():
            raise RuntimeError(f"LOCAL_ARTIFACT_MISSING: {artifact_name}")
        expected_size = int(manifest["size_bytes"][artifact_key])
        if artifact_path.stat().st_size != expected_size:
            raise RuntimeError(f"LOCAL_ARTIFACT_SIZE_MISMATCH: {artifact_name}")


def metadata_from_manifest(config: WorkerConfig, artifact_info: ArtifactInfo) -> dict:
    manifest = artifact_info.manifest
    processed = {
        "parser_profile": manifest.get("parser_profile", config.parser_profile),
        "parser_version": manifest.get("parser_version", config.parser_version),
        "chunk_count": artifact_info.chunk_count,
        "artifact_uuid": artifact_info.artifact_uuid,
    }
    if manifest.get("embedding") is not None:
        processed["embedding"] = manifest["embedding"]
    return {"processed": processed}


def s3_ready_payload_for_candidate(
    config: WorkerConfig,
    candidate: ParseFinalizationCandidate,
) -> dict:
    return {
        "collection_path": candidate.collection_path,
        "collection_storage_path": candidate.collection_storage_path,
        "processed_storage_path": candidate.processed_storage_path,
        "manifest_s3_key": processed_manifest_s3_key(config, candidate),
        "s3_bucket": config.s3_bucket,
        "s3_prefix": config.s3_processed_prefix,
        "trigger": "parse_finalization_replay",
    }


class ParseFinalizationReconciler:
    def __init__(
        self,
        config: WorkerConfig,
        limit: int = 25,
        document_id: str | None = None,
        dry_run: bool = False,
    ):
        if limit <= 0:
            raise ValueError("limit must be positive")
        self.config = config
        self.limit = limit
        self.document_id = document_id
        self.dry_run = dry_run

    def run_forever(self) -> None:
        while True:
            reconciled = self.run_once()
            if not reconciled:
                time.sleep(self.config.poll_interval_seconds)

    def run_once(self) -> int:
        with control_plane.connect(self.config.database_url) as conn:
            candidates = list_parse_finalization_candidates(conn, self.limit, self.document_id)
            reconciled = 0
            for candidate in candidates:
                if self.reconcile_candidate(conn, candidate):
                    reconciled += 1
            return reconciled

    def reconcile_candidate(self, conn, candidate: ParseFinalizationCandidate) -> bool:
        manifest_path = manifest_path_for_candidate(self.config, candidate)
        if not manifest_path.exists():
            LOGGER.info(
                "parse finalization candidate job=%s document=%s has no manifest at %s",
                candidate.job_id,
                candidate.document_id,
                manifest_path,
            )
            return False

        artifact_info = load_artifact_info(manifest_path)
        validate_manifest(candidate, artifact_info)
        validate_local_artifact_files(manifest_path, artifact_info)
        manifest_hash = canonical_manifest_hash(artifact_info.manifest)
        metadata_json = metadata_from_manifest(self.config, artifact_info)
        s3_ready_payload_json = s3_ready_payload_for_candidate(self.config, candidate)

        if self.dry_run:
            LOGGER.info(
                "parse finalization candidate job=%s document=%s is replayable from %s",
                candidate.job_id,
                candidate.document_id,
                manifest_path,
            )
            return True

        result = control_plane.replay_parse_local_ready_from_artifact(
            conn,
            candidate.job_id,
            self.config.worker_id,
            candidate.document_id,
            candidate.document_version,
            manifest_path.as_posix(),
            artifact_info.artifact_uuid,
            manifest_hash,
            artifact_info.chunk_count,
            metadata_json,
            s3_ready_payload_json,
            "processed_manifest_found_by_reconciler",
        )
        if result is None:
            LOGGER.warning(
                "parse finalization replay returned no row for job=%s document=%s status=%s document_status=%s",
                candidate.job_id,
                candidate.document_id,
                candidate.job_status,
                candidate.document_status,
            )
            return False

        if candidate.pgmq_queue and candidate.pgmq_msg_id is not None:
            queue.archive_job_message_by_id(conn, candidate.pgmq_queue, candidate.pgmq_msg_id)

        LOGGER.info(
            "replayed parse finalization job=%s document=%s s3_ready_job=%s msg=%s",
            candidate.job_id,
            candidate.document_id,
            result.s3_ready_job_id,
            result.s3_ready_msg_id,
        )
        return True
