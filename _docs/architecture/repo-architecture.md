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
lastReviewedAt: 2026-04-29
lastReviewedCommit: 09e5508f5b5391669df252fb67d8ba9a60fbf08e
---

# Unstructure Architecture

## Overview

The repository contains Python workflows for converting heterogeneous source
documents into structured chunks and index-ready artifacts for TianGong AI.
Workflows are organized by source domain under `src/**`.

## Key Paths

- `src/{ali,education,edu_textbooks,esg,journals,patents,pptx,reports,standards}/**`:
  source-domain processing scripts.
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
- Edge functions query indexes or storage populated by these workflows, but API
  serving remains outside this repository.
