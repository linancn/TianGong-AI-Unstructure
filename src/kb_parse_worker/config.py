"""Runtime configuration for the KB parse worker."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import load_dotenv


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


def _positive_int_env(name: str, default: int) -> int:
    value = _int_env(name, default)
    if value <= 0:
        raise ValueError(f"{name} must be positive.")
    return value


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def database_url_from_env() -> str:
    for name in ("DATABASE_URL", "SUPABASE_DB_URL", "KB_DATABASE_URL"):
        value = os.getenv(name)
        if value:
            return value

    db = os.getenv("POSTGRES_DB") or os.getenv("SUPABASE_DB_NAME")
    user = os.getenv("POSTGRES_USER") or os.getenv("SUPABASE_DB_USER")
    password = os.getenv("POSTGRES_PASSWORD") or os.getenv("SUPABASE_DB_PASSWORD")
    host = os.getenv("POSTGRES_HOST") or os.getenv("SUPABASE_DB_HOST") or "localhost"
    port = os.getenv("POSTGRES_PORT") or os.getenv("SUPABASE_DB_PORT") or "5432"
    if not db or not user or password is None:
        raise ValueError(
            "Set DATABASE_URL/SUPABASE_DB_URL/KB_DATABASE_URL or POSTGRES_DB, "
            "POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT. "
            "SUPABASE_DB_NAME, SUPABASE_DB_USER, SUPABASE_DB_PASSWORD, "
            "SUPABASE_DB_HOST, SUPABASE_DB_PORT are also supported."
        )
    return (
        f"postgresql://{quote_plus(user)}:{quote_plus(password)}"
        f"@{host}:{port}/{quote_plus(db)}"
    )


def load_worker_env() -> None:
    repo_env = Path(__file__).resolve().parents[2] / ".env"
    if repo_env.exists():
        load_dotenv(repo_env)
    load_dotenv()


@dataclass(frozen=True)
class WorkerConfig:
    database_url: str
    worker_id: str
    queue_name: str
    s3_ready_queue_name: str
    queue_vt_seconds: int
    lock_seconds: int
    heartbeat_interval_seconds: int
    poll_interval_seconds: int
    nas_raw_root: Path
    nas_processed_root: Path
    unstructure_serve_url: str
    unstructure_serve_bearer_token: str
    parser_profile: str
    parser_version: str
    s3_ready_mode: str
    s3_bucket: str | None
    s3_processed_prefix: str
    s3_strict_hash: bool
    s3_ready_timeout_seconds: int
    s3_ready_poll_interval_seconds: int
    embedding_base_url: str
    embedding_model: str
    embedding_api_key: str
    embedding_dimensions: int
    embedding_batch_size: int
    embedding_timeout_seconds: int
    parse_job_timeout_seconds: int
    s3_ready_job_timeout_seconds: int

    @classmethod
    def from_env(cls) -> "WorkerConfig":
        load_worker_env()
        worker_id = os.getenv("KB_PARSE_WORKER_ID") or f"{socket.gethostname()}-{os.getpid()}"
        unstructure_url = os.getenv("UNSTRUCTURE_SERVE_URL")
        token = os.getenv("UNSTRUCTURE_SERVE_BEARER_TOKEN")
        nas_raw_root = os.getenv("NAS_RAW_ROOT")
        nas_processed_root = os.getenv("NAS_PROCESSED_ROOT")
        if not unstructure_url:
            raise ValueError("UNSTRUCTURE_SERVE_URL is required.")
        if not token:
            raise ValueError("UNSTRUCTURE_SERVE_BEARER_TOKEN is required.")
        if not nas_raw_root:
            raise ValueError("NAS_RAW_ROOT is required.")
        if not nas_processed_root:
            raise ValueError("NAS_PROCESSED_ROOT is required.")
        s3_ready_timeout_seconds = _positive_int_env("KB_PARSE_S3_READY_TIMEOUT_SECONDS", 900)

        return cls(
            database_url=database_url_from_env(),
            worker_id=worker_id,
            queue_name=os.getenv("KB_PARSE_QUEUE", "kb_parse_queue"),
            s3_ready_queue_name=os.getenv("KB_S3_READY_QUEUE", "kb_s3_ready_queue"),
            queue_vt_seconds=_positive_int_env("KB_PARSE_QUEUE_VT_SECONDS", 1800),
            lock_seconds=_positive_int_env("KB_PARSE_LOCK_SECONDS", 1800),
            heartbeat_interval_seconds=_positive_int_env("KB_PARSE_HEARTBEAT_INTERVAL_SECONDS", 60),
            poll_interval_seconds=_positive_int_env("KB_PARSE_POLL_INTERVAL_SECONDS", 5),
            nas_raw_root=Path(nas_raw_root),
            nas_processed_root=Path(nas_processed_root),
            unstructure_serve_url=unstructure_url,
            unstructure_serve_bearer_token=token,
            parser_profile=os.getenv("KB_PARSE_PARSER_PROFILE", "mineru_with_images"),
            parser_version=os.getenv("KB_PARSE_PARSER_VERSION", "unstructure-serve"),
            s3_ready_mode=os.getenv("KB_PARSE_S3_READY_MODE", "check"),
            s3_bucket=os.getenv("KB_PROCESSED_S3_BUCKET", "tiangong"),
            s3_processed_prefix=os.getenv("KB_PROCESSED_S3_PREFIX", "processed_docs"),
            s3_strict_hash=_bool_env("KB_PARSE_S3_STRICT_HASH", False),
            s3_ready_timeout_seconds=s3_ready_timeout_seconds,
            s3_ready_poll_interval_seconds=_positive_int_env(
                "KB_PARSE_S3_READY_POLL_INTERVAL_SECONDS", 15
            ),
            embedding_base_url=os.getenv("KB_EMBEDDING_BASE_URL", "http://192.168.1.140:7710/v1"),
            embedding_model=os.getenv("KB_EMBEDDING_MODEL", "Qwen/Qwen3-Embedding-8B"),
            embedding_api_key=os.getenv("KB_EMBEDDING_API_KEY", "EMPTY"),
            embedding_dimensions=_positive_int_env("KB_EMBEDDING_DIMENSIONS", 1536),
            embedding_batch_size=_positive_int_env("KB_EMBEDDING_BATCH_SIZE", 32),
            embedding_timeout_seconds=_positive_int_env("KB_EMBEDDING_TIMEOUT_SECONDS", 600),
            parse_job_timeout_seconds=_positive_int_env("KB_PARSE_JOB_TIMEOUT_SECONDS", 7200),
            s3_ready_job_timeout_seconds=_positive_int_env(
                "KB_PARSE_S3_READY_JOB_TIMEOUT_SECONDS", s3_ready_timeout_seconds + 300
            ),
        )
