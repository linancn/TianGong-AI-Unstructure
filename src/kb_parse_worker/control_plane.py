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
