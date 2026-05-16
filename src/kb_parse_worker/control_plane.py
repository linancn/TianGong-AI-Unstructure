"""Supabase/Postgres control-plane RPC calls for parse jobs."""

from __future__ import annotations

from dataclasses import dataclass

import psycopg2
import psycopg2.extras


@dataclass(frozen=True)
class ClaimedJob:
    job_id: str
    document_id: str
    document_version: int
    stage: str
    status: str
    payload_json: dict


@dataclass(frozen=True)
class ClaimJobResult:
    claim_status: str
    archive_current_message: bool
    job_id: str
    document_id: str | None
    document_version: int | None
    stage: str | None
    status: str | None
    payload_json: dict
    next_retry_at: str | None
    retry_wakeup_msg_id: int | None

    @property
    def claimed(self) -> bool:
        return self.claim_status == "claimed"


@dataclass(frozen=True)
class S3ReadyEnqueueResult:
    parse_job_id: str
    parse_job_status: str
    s3_ready_job_id: str
    s3_ready_msg_id: int
    document_status: str


@dataclass(frozen=True)
class FailJobResult:
    job_id: str
    job_status: str
    document_status: str
    next_retry_at: str | None = None
    retry_wakeup_msg_id: int | None = None


def connect(database_url: str):
    return psycopg2.connect(database_url)


def claim_job(
    conn,
    job_id: str,
    queue_name: str,
    msg_id: int,
    worker_id: str,
    lock_seconds: int,
) -> ClaimJobResult | None:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            select *
            from public.claim_job_from_pgmq_message(%s, %s, %s, %s, %s)
            """,
            (job_id, queue_name, msg_id, worker_id, lock_seconds),
        )
        row = cur.fetchone()
    conn.commit()
    if row is None:
        return None
    return ClaimJobResult(
        claim_status=str(row["claim_status"]),
        archive_current_message=bool(row["archive_current_message"]),
        job_id=str(row["job_id"]),
        document_id=str(row["document_id"]) if row["document_id"] is not None else None,
        document_version=int(row["document_version"]) if row["document_version"] is not None else None,
        stage=str(row["stage"]) if row["stage"] is not None else None,
        status=str(row["status"]) if row["status"] is not None else None,
        payload_json=dict(row["payload_json"] or {}),
        next_retry_at=row["next_retry_at"].isoformat() if row["next_retry_at"] is not None else None,
        retry_wakeup_msg_id=(
            int(row["retry_wakeup_msg_id"]) if row["retry_wakeup_msg_id"] is not None else None
        ),
    )


def heartbeat_job(
    conn,
    job_id: str,
    worker_id: str,
    lock_seconds: int,
    vt_seconds: int | None = None,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "select public.heartbeat_job(%s, %s, %s, %s)",
            (job_id, worker_id, lock_seconds, vt_seconds),
        )
        row = cur.fetchone()
    conn.commit()
    return bool(row and row[0])


def fail_job(
    conn,
    job_id: str,
    worker_id: str,
    retryable: bool,
    error: str,
    error_stage: str = "parse",
) -> FailJobResult | None:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "select * from public.fail_job_v2(%s, %s, %s, %s, %s)",
            (job_id, worker_id, retryable, error[:2000], error_stage),
        )
        row = cur.fetchone()
    conn.commit()
    if row is None:
        return None
    return FailJobResult(
        job_id=str(row["job_id"]),
        job_status=str(row["job_status"]),
        document_status=str(row["document_status"]),
        next_retry_at=row["next_retry_at"].isoformat() if row["next_retry_at"] is not None else None,
        retry_wakeup_msg_id=(
            int(row["retry_wakeup_msg_id"]) if row["retry_wakeup_msg_id"] is not None else None
        ),
    )


def mark_parse_local_ready(
    conn,
    job_id: str,
    worker_id: str,
    document_id: str,
    document_version: int,
    manifest_local_uri: str,
    artifact_uuid: str,
    manifest_hash: str,
    chunk_count: int,
    metadata_json: dict,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            select public.mark_parse_local_ready(
              %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                job_id,
                worker_id,
                document_id,
                document_version,
                manifest_local_uri,
                artifact_uuid,
                manifest_hash,
                chunk_count,
                psycopg2.extras.Json(metadata_json),
            ),
        )
        row = cur.fetchone()
    conn.commit()
    return bool(row and row[0])


def mark_processed_s3_ready(
    conn,
    job_id: str,
    worker_id: str,
    document_id: str,
    document_version: int,
    manifest_s3_key: str,
    manifest_hash: str,
    artifact_uuid: str,
    manifest_local_uri: str,
    chunk_count: int,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            select public.mark_processed_s3_ready(
              %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                job_id,
                worker_id,
                document_id,
                document_version,
                manifest_s3_key,
                manifest_hash,
                artifact_uuid,
                manifest_local_uri,
                chunk_count,
            ),
        )
        row = cur.fetchone()
    conn.commit()
    return bool(row and row[0])


def complete_parse_local_ready_and_enqueue_s3_check(
    conn,
    job_id: str,
    worker_id: str,
    document_id: str,
    document_version: int,
    manifest_local_uri: str,
    artifact_uuid: str,
    manifest_hash: str,
    chunk_count: int,
    metadata_json: dict,
    s3_ready_payload_json: dict,
) -> S3ReadyEnqueueResult | None:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            select *
            from public.complete_parse_local_ready_and_enqueue_s3_check(
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                job_id,
                worker_id,
                document_id,
                document_version,
                manifest_local_uri,
                artifact_uuid,
                manifest_hash,
                chunk_count,
                psycopg2.extras.Json(metadata_json),
                psycopg2.extras.Json(s3_ready_payload_json),
            ),
        )
        row = cur.fetchone()
    conn.commit()
    if row is None:
        return None
    return S3ReadyEnqueueResult(
        parse_job_id=str(row["parse_job_id"]),
        parse_job_status=str(row["parse_job_status"]),
        s3_ready_job_id=str(row["s3_ready_job_id"]),
        s3_ready_msg_id=int(row["s3_ready_msg_id"]),
        document_status=str(row["document_status"]),
    )


def replay_parse_local_ready_from_artifact(
    conn,
    job_id: str,
    worker_id: str,
    document_id: str,
    document_version: int,
    manifest_local_uri: str,
    artifact_uuid: str,
    manifest_hash: str,
    chunk_count: int,
    metadata_json: dict,
    s3_ready_payload_json: dict,
    replay_reason: str,
) -> S3ReadyEnqueueResult | None:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            select *
            from public.replay_parse_local_ready_from_artifact(
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                job_id,
                worker_id,
                document_id,
                document_version,
                manifest_local_uri,
                artifact_uuid,
                manifest_hash,
                chunk_count,
                psycopg2.extras.Json(metadata_json),
                psycopg2.extras.Json(s3_ready_payload_json),
                replay_reason,
            ),
        )
        row = cur.fetchone()
    conn.commit()
    if row is None:
        return None
    return S3ReadyEnqueueResult(
        parse_job_id=str(row["parse_job_id"]),
        parse_job_status=str(row["parse_job_status"]),
        s3_ready_job_id=str(row["s3_ready_job_id"]),
        s3_ready_msg_id=int(row["s3_ready_msg_id"]),
        document_status=str(row["document_status"]),
    )


def complete_s3_ready_check(
    conn,
    job_id: str,
    worker_id: str,
    document_id: str,
    document_version: int,
    manifest_s3_key: str,
    manifest_hash: str,
    artifact_uuid: str,
    manifest_local_uri: str,
    chunk_count: int,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            select public.complete_s3_ready_check(
              %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                job_id,
                worker_id,
                document_id,
                document_version,
                manifest_s3_key,
                manifest_hash,
                artifact_uuid,
                manifest_local_uri,
                chunk_count,
            ),
        )
        row = cur.fetchone()
    conn.commit()
    return bool(row and row[0])
