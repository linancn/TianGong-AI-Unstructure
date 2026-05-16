"""Command line entrypoint for the KB parse worker."""

from __future__ import annotations

import argparse
import logging

from .config import WorkerConfig
from .reconciler import ParseFinalizationReconciler
from .worker import ParseWorker, S3ReadyWorker


def main() -> int:
    parser = argparse.ArgumentParser(description="Run KB parse pipeline workers.")
    parser.add_argument("mode", choices=("once", "run"), help="Run one queue message or poll forever.")
    parser.add_argument(
        "--worker",
        choices=("parse", "s3-ready", "parse-finalization-reconciler"),
        default="parse",
        help="Worker role to run.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Maximum candidates per reconciler scan.",
    )
    parser.add_argument(
        "--document-id",
        default=None,
        help="Restrict the parse finalization reconciler to one document id.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="For the parse finalization reconciler, report replayable jobs without writing DB state.",
    )
    parser.add_argument("--log-level", default="INFO", help="Python logging level.")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = WorkerConfig.from_env()
    if args.worker == "parse":
        worker = ParseWorker(config)
    elif args.worker == "s3-ready":
        worker = S3ReadyWorker(config)
    else:
        worker = ParseFinalizationReconciler(
            config,
            limit=args.limit,
            document_id=args.document_id,
            dry_run=args.dry_run,
        )
    if args.mode == "once":
        worker.run_once()
    else:
        worker.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
