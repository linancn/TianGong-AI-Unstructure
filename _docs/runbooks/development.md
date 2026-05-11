---
docType: runbook
scope: repo
status: current
authoritative: true
owner: unstructure
language: en
whenToUse: "When developing, validating, or operating unstructure processing workflows."
whenToUpdate: "When setup commands, dependencies, long-running job commands, or validation steps change."
checkPaths:
  - README.md
  - requirements.txt
  - src/**
  - docker/**
lastReviewedAt: 2026-05-10
lastReviewedCommit: 163c1c726891c17cba2ef3442e96b92af593ef6b
---

# Unstructure Development Runbook

## Setup

1. Create and activate a Python virtual environment, typically Python 3.12.
2. Install dependencies from `requirements.txt`.
3. Install native tooling required by the target workflow, such as
   `libmagic-dev`, `poppler-utils`, `libreoffice`, `pandoc`, and OCR
   dependencies.
4. Configure any cloud, vector database, or model provider credentials through
   local environment files; do not commit secrets.

## Validation

Run:

```bash
docpact validate-config --root . --strict
```

For Python script changes, run the smallest representative command for the
touched domain, or at minimum a syntax/import smoke check against the changed
script in the active virtual environment.

For the KB parse worker, run a syntax smoke check before connecting to live
queues:

```bash
python -m compileall src/kb_parse_worker
```

Run one queue message:

```bash
python -m src.kb_parse_worker.cli once
python -m src.kb_parse_worker.cli once --worker s3-ready
```

Run continuously:

```bash
python -m src.kb_parse_worker.cli run
python -m src.kb_parse_worker.cli run --worker s3-ready
```

Run continuously under PM2:

```bash
pm2 start ecosystem.kb_parse_worker.json
pm2 save
pm2 resurrect
pm2 logs kb-parse-worker
pm2 logs kb-s3-ready-worker
pm2 restart kb-parse-worker
pm2 restart kb-s3-ready-worker
pm2 stop kb-parse-worker
pm2 stop kb-s3-ready-worker
pm2 delete kb-parse-worker
pm2 delete kb-s3-ready-worker
```

The KB parse worker explicitly loads the repository-local `.env` file before
falling back to the default `.env` lookup, so it can be started from either the
repository root or the workspace root.

Current workspace live F03 workers run on the self-hosted parse worker host,
not on the local operator machine. The worker host is the machine that can read
`NAS_RAW_ROOT`, write `NAS_PROCESSED_ROOT`, reach Supabase Postgres, and call
Unstructure-Serve. Operator-side variables such as `PARSE_WORKER_HOST`,
`PARSE_WORKER_SSH_PORT`, `PARSE_WORKER_USER`, `PARSE_WORKER_SSH_HOST_ALIAS`,
`SUPABASE_DB_*`, `NAS_*`, `UNSTRUCTURE_SERVE_*`, `KB_PROCESSED_S3_*`, and AWS
credentials, plus `KB_EMBEDDING_*`, are loaded from the root workspace
`.env.ops.local` only for manual operations, SSH login, and smoke-test
preparation. Do not treat root
`.env.ops.local` as worker runtime configuration and do not copy secret values
into docs, issues, PRs, or chat.

The remote worker runtime must receive the required values from the remote
`TianGong-AI-Unstructure/.env` file or host process environment. Run live
`once`, `run`, PM2 restart, and S3-ready checks on that remote host unless the
local machine has an equivalent NAS mount and the same runtime variables.

Required runtime variables:

```text
DATABASE_URL or SUPABASE_DB_URL or KB_DATABASE_URL
or SUPABASE_DB_HOST / SUPABASE_DB_PORT / SUPABASE_DB_NAME /
SUPABASE_DB_USER / SUPABASE_DB_PASSWORD
NAS_RAW_ROOT
NAS_PROCESSED_ROOT
UNSTRUCTURE_SERVE_URL
UNSTRUCTURE_SERVE_BEARER_TOKEN
KB_EMBEDDING_BASE_URL
KB_EMBEDDING_MODEL
KB_PROCESSED_S3_BUCKET when overriding the default processed bucket
KB_S3_READY_QUEUE when overriding the default s3-ready queue
```

Current workspace worker deployment points `UNSTRUCTURE_SERVE_URL` at:

```text
UNSTRUCTURE_SERVE_URL=http://192.168.1.140:7770/mineru_with_images
```

The parse worker calls Unstructure-Serve with `return_txt=true`. It uses the
returned `result` JSON for chunk embeddings and pickle generation, drops any
returned chunk whose `text` is empty before embedding, and writes the returned
whole-document `txt` payload as `{artifact_uuid}.txt` beside
`{artifact_uuid}.pkl`.

After MinerU returns chunks, the parse worker calls the OpenAI-compatible
embedding endpoint before writing artifacts. The pickle artifact stores each
remaining chunk with an `embedding` key, while the JSONL artifact omits
embeddings to keep line-oriented inspection light. The worker requests
provider-default Qwen3-Embedding-8B vectors, then locally truncates and
normalizes them to the configured dimension:

```text
KB_EMBEDDING_BASE_URL=http://192.168.1.140:7710/v1
KB_EMBEDDING_MODEL=Qwen/Qwen3-Embedding-8B
KB_EMBEDDING_API_KEY=EMPTY
KB_EMBEDDING_DIMENSIONS=1536
KB_EMBEDDING_BATCH_SIZE=32
KB_EMBEDDING_TIMEOUT_SECONDS=600
```

Current workspace design documents point the processed S3 location at bucket
`tiangong` with prefix `processed_docs`. The worker defaults to those values and
keeps both overridable through runtime configuration:

```text
KB_PROCESSED_S3_BUCKET=tiangong
KB_PROCESSED_S3_PREFIX=processed_docs
```

Long-running parse jobs keep both the KB job lock and PGMQ visibility timeout
fresh through `heartbeat_job(...)`. The worker defaults to:

```text
KB_PARSE_HEARTBEAT_INTERVAL_SECONDS=60
KB_PARSE_JOB_TIMEOUT_SECONDS=7200
```

Raw and processed artifact paths are derived from the collection storage path.
For example, collection path `/course/thu_humanities` resolves raw files under
`course/thu_humanities/{document_id}{file_ext}` and processed artifacts under
`course_pickle/thu_humanities_pickle/{document_id}`.

The workers do not upload artifacts to S3 directly. The parse worker writes raw
inputs and processed artifacts to NAS paths, including the manifest-declared
JSONL, pickle, and optional full-text TXT artifacts, calls
`complete_parse_local_ready_and_enqueue_s3_check(...)`, archives the parse queue
message, and exits without waiting for NAS-to-S3 sync. The S3-ready worker then
waits for the NAS sync layer to publish processed artifacts to S3 before calling
`complete_s3_ready_check(...)`. Tune the S3-ready worker check wait window with:

```text
KB_PARSE_S3_READY_TIMEOUT_SECONDS=900
KB_PARSE_S3_READY_POLL_INTERVAL_SECONDS=15
KB_PARSE_S3_READY_JOB_TIMEOUT_SECONDS=1200
```

For deployments where NAS-to-S3 sync can take a long time, keep the parse worker
and S3-ready worker as separate PM2 processes. Retry attempts for the S3-ready
stage reuse `processed_manifest_local_uri` and only re-check processed S3
readiness; they do not parse the document again.

Worker failures are classified before calling `fail_job(...)`. Terminal parse
failures such as `RAW_HASH_MISMATCH`, `RAW_STORAGE_PATH_MISMATCH`, malformed
parser responses, empty parse results, and embedding schema/dimension errors are
reported with `retryable=false`. Transient parser, embedding, DB, timeout, and
network failures remain retryable. Terminal S3-ready failures include missing
local manifests, manifest identity mismatches, and strict sha256 artifact
mismatches; ordinary S3 sync delays remain retryable. When `fail_job(...)`
returns `dead`, the worker archives the PGMQ message immediately so a terminal
document does not keep resurfacing after the visibility timeout.

Use `KB_PARSE_S3_READY_MODE=skip` only for local smoke runs where processed S3
sync is intentionally unavailable.

## Long-Running Jobs

Use current script paths confirmed with `rg --files src` before starting
long-running jobs. Treat README background commands as legacy notes unless the
referenced path exists.

## Documentation Updates

Update `_docs/architecture/repo-architecture.md` when a source domain,
processing stage, output target, or dependency class changes. Update this
runbook when setup, validation, or job operation steps change.
