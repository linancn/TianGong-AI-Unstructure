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
lastReviewedAt: 2026-04-29
lastReviewedCommit: 09e5508f5b5391669df252fb67d8ba9a60fbf08e
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

## Long-Running Jobs

Use current script paths confirmed with `rg --files src` before starting
long-running jobs. Treat README background commands as legacy notes unless the
referenced path exists.

## Documentation Updates

Update `_docs/architecture/repo-architecture.md` when a source domain,
processing stage, output target, or dependency class changes. Update this
runbook when setup, validation, or job operation steps change.
