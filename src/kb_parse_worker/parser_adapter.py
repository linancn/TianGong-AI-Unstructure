"""HTTP adapter for Unstructure-Serve /mineru_with_images."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests


class ParserError(RuntimeError):
    pass


@dataclass(frozen=True)
class ParsedDocument:
    result: list[Any]
    txt: str | None
    original_chunk_count: int
    dropped_empty_text_count: int


def _has_nonempty_text(item: Any) -> bool:
    if isinstance(item, dict):
        text = item.get("text")
        return isinstance(text, str) and bool(text.strip())
    if isinstance(item, str):
        return bool(item.strip())
    return True


def filter_empty_text_chunks(result: list[Any]) -> tuple[list[Any], int]:
    filtered = [item for item in result if _has_nonempty_text(item)]
    return filtered, len(result) - len(filtered)


def parse_with_unstructure_serve(
    raw_path: Path,
    api_url: str,
    bearer_token: str,
    timeout_seconds: int = 3600,
    return_txt: bool = True,
) -> ParsedDocument:
    headers = {"Authorization": f"Bearer {bearer_token}"}
    with raw_path.open("rb") as handle:
        response = requests.post(
            api_url,
            files={"file": (raw_path.name, handle)},
            params={"return_txt": "true" if return_txt else "false"},
            data={"return_txt": "true" if return_txt else "false"},
            headers=headers,
            timeout=timeout_seconds,
        )
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise ParserError(f"parser http error {response.status_code}: {response.text[:500]}") from exc

    payload = response.json()
    if "result" not in payload:
        raise ParserError("parser response missing result")
    result = payload["result"]
    if not isinstance(result, list):
        raise ParserError("parser result must be a list")
    filtered_result, dropped_count = filter_empty_text_chunks(result)
    txt = payload.get("txt")
    if txt is not None and not isinstance(txt, str):
        raise ParserError("parser txt must be a string when present")
    if return_txt and txt is None:
        raise ParserError("parser response missing txt")
    return ParsedDocument(
        result=filtered_result,
        txt=txt,
        original_chunk_count=len(result),
        dropped_empty_text_count=dropped_count,
    )
