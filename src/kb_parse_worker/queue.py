"""PGMQ transport helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class QueueMessage:
    msg_id: int
    job_id: str
    raw_payload: dict[str, Any]


def read_one(conn, queue_name: str, vt_seconds: int) -> QueueMessage | None:
    with conn.cursor() as cur:
        cur.execute("select msg_id, message from pgmq.read(%s, %s, 1)", (queue_name, vt_seconds))
        row = cur.fetchone()
    conn.commit()
    if row is None:
        return None
    msg_id, message = row
    if isinstance(message, str):
        payload = json.loads(message)
    else:
        payload = dict(message)
    job_id = payload.get("job_id")
    if not job_id:
        raise ValueError(f"Queue message {msg_id} has no job_id.")
    return QueueMessage(msg_id=int(msg_id), job_id=str(job_id), raw_payload=payload)


def archive_job_message(conn, job_id: str, worker_id: str | None = None) -> bool:
    with conn.cursor() as cur:
        cur.execute("select public.archive_job_message(%s, %s)", (job_id, worker_id))
        row = cur.fetchone()
    conn.commit()
    return bool(row and row[0])

