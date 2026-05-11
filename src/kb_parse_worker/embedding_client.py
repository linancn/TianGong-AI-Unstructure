"""Embedding client for KB parse chunks."""

from __future__ import annotations

import json
import math
from collections.abc import Callable
from typing import Any

import requests


class EmbeddingError(RuntimeError):
    pass


def _chunk_text(item: Any) -> str:
    if not isinstance(item, dict):
        raise EmbeddingError("EMBEDDING_CHUNK_NOT_OBJECT")
    text = item.get("text", "")
    if text is None:
        return ""
    if isinstance(text, str):
        return text
    return json.dumps(text, ensure_ascii=False, sort_keys=True)


def _normalize_truncated(vector: list[Any], dimensions: int) -> list[float]:
    if len(vector) < dimensions:
        raise EmbeddingError(
            f"EMBEDDING_DIMENSION_TOO_SMALL: got {len(vector)}, need {dimensions}"
        )
    truncated = [float(value) for value in vector[:dimensions]]
    norm = math.sqrt(sum(value * value for value in truncated))
    if norm == 0:
        return truncated
    return [value / norm for value in truncated]


def _embedding_endpoint(base_url: str) -> str:
    return f"{base_url.rstrip('/')}/embeddings"


def _embed_text_batch(
    texts: list[str],
    base_url: str,
    model: str,
    api_key: str,
    dimensions: int,
    timeout_seconds: int,
) -> list[list[float]]:
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        response = requests.post(
            _embedding_endpoint(base_url),
            headers=headers,
            json={"model": model, "input": texts},
            timeout=timeout_seconds,
        )
    except requests.RequestException as exc:
        raise EmbeddingError(f"embedding request failed: {exc}") from exc
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise EmbeddingError(
            f"embedding http error {response.status_code}: {response.text[:500]}"
        ) from exc

    payload = response.json()
    data = payload.get("data")
    if not isinstance(data, list):
        raise EmbeddingError("embedding response missing data list")
    if len(data) != len(texts):
        raise EmbeddingError(
            f"embedding response count mismatch: got {len(data)}, expected {len(texts)}"
        )

    ordered = sorted(data, key=lambda item: int(item.get("index", 0)))
    vectors: list[list[float]] = []
    for item in ordered:
        embedding = item.get("embedding")
        if not isinstance(embedding, list):
            raise EmbeddingError("embedding response item missing embedding list")
        vectors.append(_normalize_truncated(embedding, dimensions))
    return vectors


def add_chunk_embeddings(
    chunks: list[Any],
    base_url: str,
    model: str,
    api_key: str,
    dimensions: int,
    batch_size: int,
    timeout_seconds: int,
    deadline_check: Callable[[], None] | None = None,
) -> list[dict[str, Any]]:
    if dimensions <= 0:
        raise ValueError("KB_EMBEDDING_DIMENSIONS must be positive")
    if batch_size <= 0:
        raise ValueError("KB_EMBEDDING_BATCH_SIZE must be positive")

    texts = [_chunk_text(item) for item in chunks]
    embedded_chunks: list[dict[str, Any]] = []
    for offset in range(0, len(chunks), batch_size):
        if deadline_check is not None:
            deadline_check()
        batch_chunks = chunks[offset : offset + batch_size]
        batch_texts = texts[offset : offset + batch_size]
        vectors = _embed_text_batch(
            batch_texts,
            base_url,
            model,
            api_key,
            dimensions,
            timeout_seconds,
        )
        for item, vector in zip(batch_chunks, vectors, strict=True):
            chunk = dict(item)
            chunk["embedding"] = vector
            embedded_chunks.append(chunk)
        if deadline_check is not None:
            deadline_check()
    return embedded_chunks
