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
lastReviewedAt: 2026-05-06
lastReviewedCommit: 17944cbd8614015728dc20b791a3e34821668234
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
Required runtime variables:

```text
DATABASE_URL or SUPABASE_DB_URL or KB_DATABASE_URL
or SUPABASE_DB_HOST / SUPABASE_DB_PORT / SUPABASE_DB_NAME /
SUPABASE_DB_USER / SUPABASE_DB_PASSWORD
NAS_RAW_ROOT
NAS_PROCESSED_ROOT
UNSTRUCTURE_SERVE_URL
UNSTRUCTURE_SERVE_BEARER_TOKEN
KB_PROCESSED_S3_BUCKET when overriding the default processed bucket
KB_S3_READY_QUEUE when overriding the default s3-ready queue
```

Current workspace worker deployment points `UNSTRUCTURE_SERVE_URL` at:

```text
UNSTRUCTURE_SERVE_URL=http://192.168.1.140:7770/mineru_with_images
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
```

Raw and processed artifact paths are derived from the collection storage path.
For example, collection path `/course/thu_humanities` resolves raw files under
`course/thu_humanities/{document_id}{file_ext}` and processed artifacts under
`course_pickle/thu_humanities_pickle/{document_id}`.

The workers do not upload artifacts to S3 directly. The parse worker writes raw
inputs and processed artifacts to NAS paths, calls
`complete_parse_local_ready_and_enqueue_s3_check(...)`, archives the parse queue
message, and exits without waiting for NAS-to-S3 sync. The S3-ready worker then
waits for the NAS sync layer to publish processed artifacts to S3 before calling
`complete_s3_ready_check(...)`. Tune the S3-ready worker check wait window with:

```text
KB_PARSE_S3_READY_TIMEOUT_SECONDS=900
KB_PARSE_S3_READY_POLL_INTERVAL_SECONDS=15
```

For deployments where NAS-to-S3 sync can take a long time, keep the parse worker
and S3-ready worker as separate PM2 processes. Retry attempts for the S3-ready
stage reuse `processed_manifest_local_uri` and only re-check processed S3
readiness; they do not parse the document again.

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
