"""Command line entrypoint for the KB parse worker."""

from __future__ import annotations

import argparse
import logging

from .config import WorkerConfig
from .worker import ParseWorker, S3ReadyWorker


def main() -> int:
    parser = argparse.ArgumentParser(description="Run KB parse or S3-ready workers.")
    parser.add_argument("mode", choices=("once", "run"), help="Run one queue message or poll forever.")
    parser.add_argument(
        "--worker",
        choices=("parse", "s3-ready"),
        default="parse",
        help="Worker role to run.",
    )
    parser.add_argument("--log-level", default="INFO", help="Python logging level.")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = WorkerConfig.from_env()
    worker = ParseWorker(config) if args.worker == "parse" else S3ReadyWorker(config)
    if args.mode == "once":
        worker.run_once()
    else:
        worker.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
