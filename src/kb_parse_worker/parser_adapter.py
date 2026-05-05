"""HTTP adapter for Unstructure-Serve /mineru_with_images."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import requests


class ParserError(RuntimeError):
    pass


def parse_with_unstructure_serve(
    raw_path: Path,
    api_url: str,
    bearer_token: str,
    timeout_seconds: int = 3600,
) -> list[Any]:
    headers = {"Authorization": f"Bearer {bearer_token}"}
    with raw_path.open("rb") as handle:
        response = requests.post(
            api_url,
            files={"file": (raw_path.name, handle)},
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
    return result

