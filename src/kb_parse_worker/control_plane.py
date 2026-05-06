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
class S3ReadyEnqueueResult:
    parse_job_id: str
    parse_job_status: str
    s3_ready_job_id: str
    s3_ready_msg_id: int
    document_status: str


def connect(database_url: str):
    return psycopg2.connect(database_url)


def claim_job(
    conn,
    job_id: str,
    queue_name: str,
    msg_id: int,
    worker_id: str,
    lock_seconds: int,
) -> ClaimedJob | None:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            select *
            from public.start_job_from_pgmq_message(%s, %s, %s, %s, %s)
            """,
            (job_id, queue_name, msg_id, worker_id, lock_seconds),
        )
        row = cur.fetchone()
    conn.commit()
    if row is None:
        return None
    return ClaimedJob(
        job_id=str(row["job_id"]),
        document_id=str(row["document_id"]),
        document_version=int(row["document_version"]),
        stage=str(row["stage"]),
        status=str(row["status"]),
        payload_json=dict(row["payload_json"] or {}),
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
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.fail_job(%s, %s, %s, %s, %s)",
            (job_id, worker_id, retryable, error[:2000], error_stage),
        )
    conn.commit()


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
