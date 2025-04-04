import argparse
import os
import subprocess
from datetime import datetime
from pathlib import Path

import yaml
from isodate import parse_duration


def parse_config(config_path):
    with open(config_path) as f:
        return yaml.safe_load(f)


def build_valid_interval_ids(config):
    start = datetime.fromisoformat(config["start"])
    end = datetime.fromisoformat(config["end"])
    step = parse_duration(config["step"])

    interval_ids = []
    t = start
    while t < end:
        ts_start = t.strftime("%Y%m%dT%H%M")
        ts_end = (t + step).strftime("%Y%m%dT%H%M")
        interval_ids.append(f"{ts_start}-{ts_end}")
        t += step
    return interval_ids


def run_pipeline():
    parser = argparse.ArgumentParser(description="Run GRIB to Zarr indexing pipeline")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--cores", default="all", help="Number of cores to use")
    parser.add_argument(
        "--timestamp", help="Start time of interval to process (e.g. 20250301T0100)"
    )
    parser.add_argument(
        "--collection", help="Optional collection name (e.g. height_levels)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate the workflow without executing tasks",
    )

    args = parser.parse_args()
    config_path = Path(args.config).resolve()
    config = parse_config(config_path)
    step = parse_duration(config["step"])
    valid_intervals = build_valid_interval_ids(config)

    os.environ["CONFIG_FILE"] = str(config_path)

    snakefile_path = Path(__file__).parent / "snakefile" / "Snakefile"

    cmd = [
        "snakemake",
        "--snakefile",
        str(snakefile_path),
        "--configfile",
        str(config_path),
        "--cores",
        args.cores,
    ]

    if args.dry_run:
        cmd.append("--dry-run")

    if args.timestamp:
        try:
            start_time = datetime.fromisoformat(args.timestamp)
        except ValueError:
            raise ValueError(f"Invalid timestamp format: {args.timestamp}")
        end_time = start_time + step
        interval_id = (
            f"{start_time.strftime('%Y%m%dT%H%M')}-{end_time.strftime('%Y%m%dT%H%M')}"
        )
        if interval_id not in valid_intervals:
            raise ValueError(
                f"Interval '{interval_id}' not in configured time range.\n"
                f"Valid range: {valid_intervals[0]} to {valid_intervals[-1]}"
            )

        if args.collection:
            cmd.append(f"zarr/{args.collection}/{interval_id}.zarr")
        else:
            for col in config["data_collection"]:
                cmd.append(f"zarr/{col}/{interval_id}.zarr")

    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    run_pipeline()
