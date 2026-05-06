---
docType: architecture
scope: repo
status: current
authoritative: true
owner: unstructure
language: en
whenToUse: "When changing document processing scripts, dependencies, or local service stack files."
whenToUpdate: "When source domains, data flow, index targets, OCR/model dependencies, or runtime assumptions change."
checkPaths:
  - src/**
  - docker/**
  - requirements.txt
lastReviewedAt: 2026-05-06
lastReviewedCommit: 17944cbd8614015728dc20b791a3e34821668234
---

# Unstructure Architecture

## Overview

The repository contains Python workflows for converting heterogeneous source
documents into structured chunks and index-ready artifacts for TianGong AI.
Workflows are organized by source domain under `src/**`.

## Key Paths

- `src/{ali,education,edu_textbooks,esg,journals,patents,pptx,reports,standards}/**`:
  source-domain processing scripts.
- `src/kb_parse_worker/**`: KB F03 parse and S3-ready workers. The parse
  worker consumes `kb_parse_queue`, claims jobs through the KB control plane,
  calls Unstructure-Serve, publishes processed artifacts to NAS, and enqueues
  the S3-ready check. The S3-ready worker consumes `kb_s3_ready_queue` and
  marks processed artifacts ready after S3 verification.
- `ecosystem.kb_parse_worker.json`: PM2 process definitions for the KB parse
  worker and S3-ready worker.
- `src/journals/**`: journal workflows; also read `src/journals/AGENTS.md`.
- `src/tools/**` and per-domain `tools/**`: helper modules.
- `src/weaviate/**`: local Weaviate utility scripts.
- `src/legacy/**`: legacy experiments and migration references; verify before reuse.
- `docker/weaviate/*.yml`: local Weaviate stacks.
- `requirements.txt`: Python dependency facts.

## Runtime Shape

The repository expects Python virtual environments and native document tooling
such as poppler, libreoffice, pandoc, tesseract, and CUDA-capable optional
components for some workloads. Treat README command dumps as legacy notes unless
the referenced files still exist.

## Integration Points

- Output artifacts feed downstream knowledge-base and search/index services.
- The KB parse worker reads raw document locations from `kb_documents.raw_uri`,
  requires raw files to live under the collection-derived storage path using
  the renamed canonical filename `{document_id}{file_ext}`, writes processed
  `jsonl`/`pkl`/`manifest.json` artifacts under the configured NAS processed
  root using a `_pickle` suffixed path derived from the collection storage
  path, and calls `complete_parse_local_ready_and_enqueue_s3_check(...)`.
  That RPC completes the parse job, leaves the document in `s3_sync_pending`,
  and enqueues a durable `s3_ready` job. A separate S3-ready worker then
  verifies the processed manifest/jsonl/pkl objects after NAS-to-S3 sync and
  calls `complete_s3_ready_check(...)` to mark `processed_s3_ready`. PM2 keeps
  both long-running worker processes resident; each worker's heartbeat loop
  keeps its active job lock and PGMQ visibility timeout fresh.
- Edge functions query indexes or storage populated by these workflows, but API
  serving remains outside this repository.
