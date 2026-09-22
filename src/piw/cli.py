from __future__ import annotations

import argparse
from pathlib import Path

from .core import build_report


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(
        description="Build the deterministic BPIC12 Slice 0 report."
    )
    command.add_argument(
        "--source",
        type=Path,
        default=Path("data/raw/BPI_Challenge_2012.xes.gz"),
    )
    command.add_argument(
        "--database",
        type=Path,
        default=Path("data/processed/bpic2012.duckdb"),
    )
    command.add_argument(
        "--report",
        type=Path,
        default=Path("evidence/slice0/bpic2012-core-report.json"),
    )
    command.add_argument(
        "--sla-threshold-ms",
        type=int,
        default=168 * 60 * 60 * 1_000,
        help="Configured test threshold only; default is 168 hours.",
    )
    return command


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if not args.source.is_file():
        parser().error(f"source file does not exist: {args.source}")
    report = build_report(
        source_path=args.source,
        database_path=args.database,
        report_path=args.report,
        sla_threshold_ms=args.sla_threshold_ms,
    )
    status = report["validation"]["status"]
    print(
        f"{status}: cases={report['raw_summary']['case_count']} "
        f"raw_events={report['raw_summary']['event_count']} "
        f"analysis_events={report['analysis_event_count']} report={args.report}"
    )
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
