from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.kb_parse_worker.artifacts import write_processed_artifacts
from src.kb_parse_worker.parser_adapter import (
    ParserError,
    filter_empty_text_chunks,
    parse_with_unstructure_serve,
)
from src.kb_parse_worker.snapshot import ParseSnapshot


def snapshot() -> ParseSnapshot:
    return ParseSnapshot(
        job_id="job-1",
        document_id="00000000-0000-0000-0000-000000000001",
        document_version=1,
        document_status="parse_queued",
        raw_uri="nas://kb/raw/course/demo/doc.pdf",
        raw_storage_region=None,
        file_ext=".pdf",
        file_size=1,
        sha256="sha",
        original_filename="doc.pdf",
        primary_collection_id="00000000-0000-0000-0000-000000000002",
        collection_name="demo",
        collection_path="/course/demo",
        collection_storage_path="course/demo",
        processed_storage_path="course_pickle/demo_pickle",
        content_type="application/pdf",
        collection_metadata_schema_json={},
        document_metadata_json={},
        job_payload_json={},
        processed_manifest_local_uri=None,
        processed_manifest_hash=None,
        processed_artifact_uuid=None,
        chunk_count=None,
    )


class KbParseWorkerArtifactTests(unittest.TestCase):
    class FakeResponse:
        status_code = 200
        text = ""

        def __init__(self, payload: dict) -> None:
            self.payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return self.payload

    def test_filter_empty_text_chunks_drops_empty_text_dicts(self) -> None:
        result, dropped = filter_empty_text_chunks(
            [
                {"text": " kept ", "page_number": 1},
                {"text": "", "page_number": 2},
                {"text": "   ", "page_number": 3},
                {"content": "missing text", "page_number": 4},
            ]
        )

        self.assertEqual(dropped, 3)
        self.assertEqual(result, [{"text": " kept ", "page_number": 1}])

    def test_parse_with_unstructure_serve_requests_txt_and_filters_result(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".pdf") as raw_file:
            raw_file.write(b"%PDF")
            raw_file.flush()
            response = self.FakeResponse(
                {
                    "result": [{"text": "chunk"}, {"text": ""}, {"text": "   "}],
                    "txt": "whole document text",
                }
            )

            with patch("src.kb_parse_worker.parser_adapter.requests.post", return_value=response) as post:
                parsed = parse_with_unstructure_serve(
                    Path(raw_file.name),
                    "https://parser.test/mineru",
                    "token",
                )

            self.assertEqual(parsed.result, [{"text": "chunk"}])
            self.assertEqual(parsed.txt, "whole document text")
            self.assertEqual(parsed.original_chunk_count, 3)
            self.assertEqual(parsed.dropped_empty_text_count, 2)
            self.assertEqual(post.call_args.kwargs["params"], {"return_txt": "true"})
            self.assertEqual(post.call_args.kwargs["data"], {"return_txt": "true"})

    def test_parse_with_unstructure_serve_requires_txt_by_default(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".pdf") as raw_file:
            raw_file.write(b"%PDF")
            raw_file.flush()
            response = self.FakeResponse({"result": [{"text": "chunk"}]})

            with patch("src.kb_parse_worker.parser_adapter.requests.post", return_value=response):
                with self.assertRaisesRegex(ParserError, "missing txt"):
                    parse_with_unstructure_serve(
                        Path(raw_file.name),
                        "https://parser.test/mineru",
                        "token",
                    )

    def test_write_processed_artifacts_writes_full_text_txt(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            final_dir, artifact_info = write_processed_artifacts(
                [{"text": "chunk", "page_number": 1, "embedding": [0.1, 0.2]}],
                snapshot(),
                Path(temp_dir),
                "profile",
                "version",
                {"model": "embedding"},
                "whole document text",
            )

            manifest_path = final_dir / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            txt_name = manifest["artifacts"]["full_text_txt"]

            self.assertEqual(txt_name, f"{manifest['artifact_uuid']}.txt")
            self.assertEqual((final_dir / txt_name).read_text(encoding="utf-8"), "whole document text")
            self.assertEqual(artifact_info.txt_name, txt_name)
            self.assertEqual(manifest["size_bytes"]["full_text_txt"], len("whole document text"))


if __name__ == "__main__":
    unittest.main()
