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
lastReviewedAt: 2026-05-05
lastReviewedCommit: f04e08fbe75d92c5ec518e8f043cdd2045c67bcd
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
```

Run continuously:

```bash
python -m src.kb_parse_worker.cli run
```

Run continuously under PM2:

```bash
pm2 start ecosystem.kb_parse_worker.json
pm2 save
pm2 resurrect
pm2 logs kb-parse-worker
pm2 restart kb-parse-worker
pm2 stop kb-parse-worker
pm2 delete kb-parse-worker
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

Processed artifact paths are derived from the collection storage path by adding
`_pickle` to each path segment. For example, collection path
`/course/thu_humanities` resolves raw files under `course/thu_humanities` and
processed artifacts under `course_pickle/thu_humanities_pickle`.

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
