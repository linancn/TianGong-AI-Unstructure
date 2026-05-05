"""KB parse worker orchestration."""

from __future__ import annotations

import hashlib
import logging
import time
from pathlib import Path

from . import control_plane, queue
from .artifacts import write_processed_artifacts
from .config import WorkerConfig
from .parser_adapter import parse_with_unstructure_serve
from .s3_ready import processed_manifest_key, wait_for_s3_processed_ready
from .snapshot import load_parse_snapshot, resolve_raw_path

LOGGER = logging.getLogger(__name__)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _validate_raw_file(path: Path, expected_size: int | None, expected_sha256: str) -> None:
    if not path.exists():
        raise RuntimeError("RAW_NOT_FOUND")
    if expected_size is not None and path.stat().st_size != expected_size:
        raise RuntimeError("RAW_SIZE_MISMATCH")
    if _sha256(path).lower() != expected_sha256.lower():
        raise RuntimeError("RAW_HASH_MISMATCH")


class ParseWorker:
    def __init__(self, config: WorkerConfig):
        self.config = config

    def run_forever(self) -> None:
        while True:
            processed = self.run_once()
            if not processed:
                time.sleep(self.config.poll_interval_seconds)

    def run_once(self) -> bool:
        with control_plane.connect(self.config.database_url) as conn:
            message = queue.read_one(conn, self.config.queue_name, self.config.queue_vt_seconds)
            if message is None:
                return False
            self.process_message(conn, message)
            return True

    def process_message(self, conn, message: queue.QueueMessage) -> None:
        claimed = control_plane.claim_job(
            conn,
            message.job_id,
            self.config.queue_name,
            message.msg_id,
            self.config.worker_id,
            self.config.lock_seconds,
        )
        if claimed is None:
            LOGGER.info("job %s was not claimable", message.job_id)
            queue.archive_job_message(conn, message.job_id, self.config.worker_id)
            return
        if claimed.stage != "parse":
            control_plane.fail_job(conn, claimed.job_id, self.config.worker_id, False, "UNSUPPORTED_STAGE")
            queue.archive_job_message(conn, claimed.job_id, self.config.worker_id)
            return

        try:
            control_plane.heartbeat_job(
                conn,
                claimed.job_id,
                self.config.worker_id,
                self.config.lock_seconds,
                self.config.queue_vt_seconds,
            )
            snapshot = load_parse_snapshot(conn, claimed.job_id)
            raw_path = resolve_raw_path(snapshot.raw_uri, self.config.nas_raw_root)
            _validate_raw_file(raw_path, snapshot.file_size, snapshot.sha256)

            result = parse_with_unstructure_serve(
                raw_path,
                self.config.unstructure_serve_url,
                self.config.unstructure_serve_bearer_token,
            )
            final_dir, artifact_info = write_processed_artifacts(
                result,
                snapshot,
                self.config.nas_processed_root,
                self.config.parser_profile,
                self.config.parser_version,
            )
            manifest_local_uri = final_dir.joinpath("manifest.json").as_posix()

            if self.config.s3_ready_mode == "skip":
                if not self.config.s3_bucket:
                    manifest_s3_key = processed_manifest_key(self.config.s3_processed_prefix, snapshot)
                else:
                    manifest_s3_key = processed_manifest_key(self.config.s3_processed_prefix, snapshot)
            else:
                if not self.config.s3_bucket:
                    raise RuntimeError("S3_NOT_READY: KB_PROCESSED_S3_BUCKET is required")
                ready = wait_for_s3_processed_ready(
                    snapshot,
                    artifact_info,
                    self.config.s3_bucket,
                    self.config.s3_processed_prefix,
                    self.config.s3_strict_hash,
                )
                manifest_s3_key = ready.manifest_s3_key

            ok = control_plane.mark_processed_s3_ready(
                conn,
                claimed.job_id,
                claimed.document_id,
                claimed.document_version,
                manifest_s3_key,
                artifact_info.manifest_hash,
                artifact_info.artifact_uuid,
                manifest_local_uri,
                artifact_info.chunk_count,
            )
            if not ok:
                raise RuntimeError("mark_processed_s3_ready returned false")
            queue.archive_job_message(conn, claimed.job_id, self.config.worker_id)
        except Exception as exc:
            LOGGER.exception("parse job %s failed", claimed.job_id)
            retryable = str(exc) not in {"RAW_HASH_MISMATCH", "EMPTY_RESULT"}
            control_plane.fail_job(conn, claimed.job_id, self.config.worker_id, retryable, str(exc))
