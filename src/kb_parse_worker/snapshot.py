"""Read-only current-state snapshot for a claimed parse job."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

import psycopg2.extras


@dataclass(frozen=True)
class ParseSnapshot:
    job_id: str
    document_id: str
    document_version: int
    raw_uri: str
    raw_storage_region: str | None
    file_ext: str | None
    file_size: int | None
    sha256: str
    original_filename: str
    primary_collection_id: str
    collection_name: str
    collection_path: str
    collection_storage_path: str
    content_type: str | None
    collection_metadata_schema_json: dict
    document_metadata_json: dict
    job_payload_json: dict


def collection_storage_path(collection_path: str) -> str:
    stripped = re.sub(r"/+", "/", collection_path.strip("/"))
    parts = [part for part in stripped.split("/") if part]
    if not parts:
        raise ValueError("collection path cannot resolve to an empty storage path")
    if any(part in {".", ".."} for part in parts):
        raise ValueError(f"collection path contains unsafe segment: {collection_path}")
    return "/".join(parts)


def load_parse_snapshot(conn, job_id: str) -> ParseSnapshot:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            select
              j.id as job_id,
              j.payload_json as job_payload_json,
              d.id as document_id,
              d.document_version,
              d.raw_uri,
              d.raw_storage_region,
              d.file_ext,
              d.file_size,
              d.sha256,
              d.original_filename,
              d.primary_collection_id,
              d.metadata_json as document_metadata_json,
              c.name as collection_name,
              c.path as collection_path,
              c.content_type,
              c.metadata_schema_json as collection_metadata_schema_json
            from public.kb_jobs j
            join public.kb_documents d on d.id = j.document_id
            join public.kb_collections c on c.id = d.primary_collection_id
            where j.id = %s
              and j.stage = 'parse'
              and j.status = 'running'
              and d.deleted_at is null
              and j.document_version = d.document_version
            """,
            (job_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"No runnable parse snapshot for job {job_id}.")
    if not row["raw_uri"]:
        raise RuntimeError(f"Document {row['document_id']} has no raw_uri.")

    path = str(row["collection_path"])
    return ParseSnapshot(
        job_id=str(row["job_id"]),
        document_id=str(row["document_id"]),
        document_version=int(row["document_version"]),
        raw_uri=str(row["raw_uri"]),
        raw_storage_region=row["raw_storage_region"],
        file_ext=row["file_ext"],
        file_size=row["file_size"],
        sha256=str(row["sha256"]),
        original_filename=str(row["original_filename"]),
        primary_collection_id=str(row["primary_collection_id"]),
        collection_name=str(row["collection_name"]),
        collection_path=path,
        collection_storage_path=collection_storage_path(path),
        content_type=row["content_type"],
        collection_metadata_schema_json=dict(row["collection_metadata_schema_json"] or {}),
        document_metadata_json=dict(row["document_metadata_json"] or {}),
        job_payload_json=dict(row["job_payload_json"] or {}),
    )


def resolve_raw_path(raw_uri: str, nas_raw_root: Path) -> Path:
    parsed = urlparse(raw_uri)
    if parsed.scheme == "":
        return Path(raw_uri)
    if parsed.scheme == "file":
        return Path(unquote(parsed.path))
    if parsed.scheme == "nas":
        rel = unquote(parsed.path).lstrip("/")
        if parsed.netloc and parsed.netloc not in {"kb", "raw"}:
            rel = f"{parsed.netloc}/{rel}" if rel else parsed.netloc
        if rel.startswith("raw/"):
            rel = rel[len("raw/") :]
        return nas_raw_root / rel
    raise ValueError(f"Unsupported raw_uri scheme: {parsed.scheme}")

