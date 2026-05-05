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
lastReviewedAt: 2026-05-05
lastReviewedCommit: d9aa38132146c069372584dc658748e61bb6b551
---

# Unstructure Architecture

## Overview

The repository contains Python workflows for converting heterogeneous source
documents into structured chunks and index-ready artifacts for TianGong AI.
Workflows are organized by source domain under `src/**`.

## Key Paths

- `src/{ali,education,edu_textbooks,esg,journals,patents,pptx,reports,standards}/**`:
  source-domain processing scripts.
- `src/kb_parse_worker/**`: KB F03 parse worker that consumes
  `kb_parse_queue`, claims jobs through the KB control plane, calls
  Unstructure-Serve, and publishes processed artifacts.
- `ecosystem.kb_parse_worker.json`: PM2 process definition for the KB parse
  worker.
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
  writes processed `jsonl`/`pkl`/`manifest.json` artifacts under the configured
  NAS processed root using a `_pickle` suffixed path derived from the collection
  storage path, records parse local-ready through KB RPCs, and marks
  `processed_s3_ready` through worker-lock-checked KB RPCs after S3 ready
  checks. PM2 keeps the long-running worker process resident; the worker's
  heartbeat loop keeps the job lock and PGMQ visibility timeout fresh while a
  document is being parsed.
- Edge functions query indexes or storage populated by these workflows, but API
  serving remains outside this repository.
