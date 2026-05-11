from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from src.kb_parse_worker import control_plane, queue
from src.kb_parse_worker.worker import (
    JobTimeout,
    ParseWorker,
    S3ReadyWorker,
    is_parse_failure_retryable,
    is_s3_ready_failure_retryable,
)


class NoopLease:
    def __init__(self, *_args, **_kwargs) -> None:
        pass

    def __enter__(self) -> "NoopLease":
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        return None

    def check(self) -> None:
        return None


def worker_config() -> SimpleNamespace:
    return SimpleNamespace(
        database_url="postgresql://test",
        worker_id="worker-1",
        queue_name="kb_parse_queue",
        s3_ready_queue_name="kb_s3_ready_queue",
        queue_vt_seconds=30,
        lock_seconds=30,
        heartbeat_interval_seconds=1,
        poll_interval_seconds=1,
        parse_job_timeout_seconds=60,
        s3_ready_job_timeout_seconds=60,
        s3_ready_mode="check",
        s3_bucket="tiangong",
        s3_processed_prefix="processed_docs",
        s3_strict_hash=False,
        s3_ready_timeout_seconds=30,
        s3_ready_poll_interval_seconds=1,
    )


def claimed(stage: str) -> control_plane.ClaimedJob:
    return control_plane.ClaimedJob(
        job_id="job-1",
        document_id="00000000-0000-0000-0000-000000000001",
        document_version=1,
        stage=stage,
        status="running",
        payload_json={},
    )


def message() -> queue.QueueMessage:
    return queue.QueueMessage(msg_id=1, job_id="job-1", raw_payload={"job_id": "job-1"})


class KbParseWorkerReliabilityTests(unittest.TestCase):
    def test_parse_failure_classifier_distinguishes_terminal_and_transient(self) -> None:
        self.assertFalse(is_parse_failure_retryable(RuntimeError("EMPTY_RESULT")))
        self.assertFalse(is_parse_failure_retryable(RuntimeError("RAW_STORAGE_PATH_MISMATCH: path")))
        self.assertTrue(is_parse_failure_retryable(RuntimeError("temporary parser outage")))
        self.assertTrue(is_parse_failure_retryable(JobTimeout("PARSE_JOB_TIMEOUT_AFTER_60s")))

    def test_s3_ready_failure_classifier_distinguishes_terminal_and_transient(self) -> None:
        self.assertFalse(is_s3_ready_failure_retryable(RuntimeError("LOCAL_MANIFEST_MISSING")))
        self.assertFalse(
            is_s3_ready_failure_retryable(RuntimeError("S3_MANIFEST_MISMATCH: document_id"))
        )
        self.assertFalse(
            is_s3_ready_failure_retryable(RuntimeError("S3_ARTIFACT_MISMATCH: chunks.pkl sha256"))
        )
        self.assertTrue(
            is_s3_ready_failure_retryable(RuntimeError("S3_NOT_READY_AFTER_TIMEOUT: not synced"))
        )
        self.assertTrue(is_s3_ready_failure_retryable(JobTimeout("S3_READY_JOB_TIMEOUT_AFTER_60s")))

    def test_parse_terminal_failure_marks_non_retryable_and_archives_dead_job(self) -> None:
        conn = object()
        fail_result = control_plane.FailJobResult(
            job_id="job-1",
            job_status="dead",
            document_status="failed",
        )
        with (
            patch("src.kb_parse_worker.worker.LeaseMaintainer", NoopLease),
            patch("src.kb_parse_worker.worker.control_plane.claim_job", return_value=claimed("parse")),
            patch("src.kb_parse_worker.worker.load_parse_snapshot", side_effect=RuntimeError("EMPTY_RESULT")),
            patch("src.kb_parse_worker.worker.control_plane.fail_job", return_value=fail_result) as fail_job,
            patch("src.kb_parse_worker.worker.queue.archive_job_message", return_value=True) as archive,
            patch("src.kb_parse_worker.worker.LOGGER.exception"),
        ):
            ParseWorker(worker_config()).process_message(conn, message())

        fail_job.assert_called_once()
        self.assertFalse(fail_job.call_args.args[3])
        archive.assert_called_once_with(conn, "job-1", "worker-1")

    def test_parse_timeout_marks_retryable_without_archiving_failed_job(self) -> None:
        conn = object()
        fail_result = control_plane.FailJobResult(
            job_id="job-1",
            job_status="failed",
            document_status="parse_queued",
        )
        with (
            patch("src.kb_parse_worker.worker.LeaseMaintainer", NoopLease),
            patch("src.kb_parse_worker.worker.control_plane.claim_job", return_value=claimed("parse")),
            patch(
                "src.kb_parse_worker.worker.load_parse_snapshot",
                side_effect=JobTimeout("PARSE_JOB_TIMEOUT_AFTER_60s"),
            ),
            patch("src.kb_parse_worker.worker.control_plane.fail_job", return_value=fail_result) as fail_job,
            patch("src.kb_parse_worker.worker.queue.archive_job_message", return_value=True) as archive,
            patch("src.kb_parse_worker.worker.LOGGER.exception"),
        ):
            ParseWorker(worker_config()).process_message(conn, message())

        fail_job.assert_called_once()
        self.assertTrue(fail_job.call_args.args[3])
        archive.assert_not_called()

    def test_s3_ready_terminal_failure_marks_non_retryable_and_archives_dead_job(self) -> None:
        conn = object()
        fail_result = control_plane.FailJobResult(
            job_id="job-1",
            job_status="dead",
            document_status="failed",
        )
        with (
            patch("src.kb_parse_worker.worker.LeaseMaintainer", NoopLease),
            patch(
                "src.kb_parse_worker.worker.control_plane.claim_job",
                return_value=claimed("s3_ready"),
            ),
            patch(
                "src.kb_parse_worker.worker.load_s3_ready_snapshot",
                return_value=SimpleNamespace(processed_manifest_local_uri=None),
            ),
            patch("src.kb_parse_worker.worker.control_plane.fail_job", return_value=fail_result) as fail_job,
            patch("src.kb_parse_worker.worker.queue.archive_job_message", return_value=True) as archive,
            patch("src.kb_parse_worker.worker.LOGGER.exception"),
        ):
            S3ReadyWorker(worker_config()).process_message(conn, message())

        fail_job.assert_called_once()
        self.assertFalse(fail_job.call_args.args[3])
        archive.assert_called_once()

    def test_s3_ready_transient_failure_marks_retryable_without_archiving_failed_job(self) -> None:
        conn = object()
        fail_result = control_plane.FailJobResult(
            job_id="job-1",
            job_status="failed",
            document_status="s3_ready_check_failed",
        )
        with (
            patch("src.kb_parse_worker.worker.LeaseMaintainer", NoopLease),
            patch(
                "src.kb_parse_worker.worker.control_plane.claim_job",
                return_value=claimed("s3_ready"),
            ),
            patch(
                "src.kb_parse_worker.worker.load_s3_ready_snapshot",
                side_effect=RuntimeError("S3_NOT_READY_AFTER_TIMEOUT: sync delay"),
            ),
            patch("src.kb_parse_worker.worker.control_plane.fail_job", return_value=fail_result) as fail_job,
            patch("src.kb_parse_worker.worker.queue.archive_job_message", return_value=True) as archive,
            patch("src.kb_parse_worker.worker.LOGGER.exception"),
        ):
            S3ReadyWorker(worker_config()).process_message(conn, message())

        fail_job.assert_called_once()
        self.assertTrue(fail_job.call_args.args[3])
        archive.assert_not_called()


if __name__ == "__main__":
    unittest.main()
