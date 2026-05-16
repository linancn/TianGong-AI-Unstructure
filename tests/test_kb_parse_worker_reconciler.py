from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.kb_parse_worker import control_plane
from src.kb_parse_worker.reconciler import (
    ParseFinalizationCandidate,
    ParseFinalizationReconciler,
    canonical_manifest_hash,
    manifest_path_for_candidate,
    processed_manifest_s3_key,
)


def config(processed_root: Path) -> SimpleNamespace:
    return SimpleNamespace(
        database_url="postgresql://test",
        worker_id="worker-1",
        poll_interval_seconds=1,
        nas_processed_root=processed_root,
        parser_profile="mineru_with_images",
        parser_version="unstructure-serve",
        s3_bucket="tiangong",
        s3_processed_prefix="processed_docs",
    )


def candidate() -> ParseFinalizationCandidate:
    return ParseFinalizationCandidate(
        job_id="11111111-1111-1111-1111-111111111111",
        document_id="22222222-2222-2222-2222-222222222222",
        document_version=3,
        job_status="dead",
        document_status="failed",
        collection_path="/course/thu_humanities",
        collection_storage_path="course/thu_humanities",
        processed_storage_path="course_pickle/thu_humanities_pickle",
        pgmq_queue="kb_parse_queue",
        pgmq_msg_id=404,
        updated_at=None,
    )


def write_manifest(root: Path, item: ParseFinalizationCandidate) -> dict:
    manifest = {
        "document_id": item.document_id,
        "document_version": item.document_version,
        "artifact_uuid": "33333333-3333-3333-3333-333333333333",
        "collection_id": "44444444-4444-4444-4444-444444444444",
        "collection_path": item.collection_path,
        "collection_storage_path": item.collection_storage_path,
        "processed_storage_path": item.processed_storage_path,
        "chunk_count": 12,
        "artifacts": {
            "chunks_jsonl": "33333333-3333-3333-3333-333333333333.jsonl",
            "chunks_pkl": "33333333-3333-3333-3333-333333333333.pkl",
        },
        "sha256": {
            "chunks_jsonl": "a" * 64,
            "chunks_pkl": "b" * 64,
        },
        "size_bytes": {
            "chunks_jsonl": 10,
            "chunks_pkl": 20,
        },
        "parser_profile": "mineru_with_images",
        "parser_version": "unstructure-serve",
        "embedding": {"model": "Qwen/Qwen3-Embedding-8B", "dimensions": 1536},
    }
    path = manifest_path_for_candidate(config(root), item)
    path.parent.mkdir(parents=True)
    path.parent.joinpath(manifest["artifacts"]["chunks_jsonl"]).write_text("x" * 10, encoding="utf-8")
    path.parent.joinpath(manifest["artifacts"]["chunks_pkl"]).write_bytes(b"x" * 20)
    path.write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
    return manifest


class ParseFinalizationReconcilerTests(unittest.TestCase):
    def test_canonical_manifest_hash_matches_worker_manifest_hash(self) -> None:
        manifest = {"b": 2, "a": "清华"}
        expected = hashlib.sha256(
            json.dumps(manifest, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        self.assertEqual(canonical_manifest_hash(manifest), expected)

    def test_replays_existing_manifest_and_archives_stale_parse_message(self) -> None:
        item = candidate()
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest = write_manifest(Path(tmp_dir), item)
            worker = ParseFinalizationReconciler(config(Path(tmp_dir)), limit=1)
            replay_result = control_plane.S3ReadyEnqueueResult(
                parse_job_id=item.job_id,
                parse_job_status="succeeded",
                s3_ready_job_id="55555555-5555-5555-5555-555555555555",
                s3_ready_msg_id=99,
                document_status="s3_sync_pending",
            )
            with (
                patch(
                    "src.kb_parse_worker.reconciler.control_plane.replay_parse_local_ready_from_artifact",
                    return_value=replay_result,
                ) as replay,
                patch(
                    "src.kb_parse_worker.reconciler.queue.archive_job_message_by_id",
                    return_value=True,
                ) as archive,
            ):
                conn = object()
                self.assertTrue(worker.reconcile_candidate(conn, item))

            replay.assert_called_once()
            call_args = replay.call_args.args
            self.assertEqual(call_args[1], item.job_id)
            self.assertEqual(call_args[3], item.document_id)
            self.assertEqual(call_args[6], manifest["artifact_uuid"])
            self.assertEqual(call_args[10]["manifest_s3_key"], processed_manifest_s3_key(worker.config, item))
            self.assertEqual(call_args[9]["processed"]["embedding"], manifest["embedding"])
            archive.assert_called_once_with(conn, item.pgmq_queue, item.pgmq_msg_id)


if __name__ == "__main__":
    unittest.main()
