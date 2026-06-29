#! /usr/bin/python3.11
import argparse
import subprocess
import sys
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_SCRIPT = Path(__file__).with_name("instance_validation_dashboard.py")
TABLE_FILES = {
    "stoppoints.txt",
    "line.txt",
    "servicejourney.txt",
    "deadruntime.txt",
    "connections.txt",
}


def resolve_project_path(path):
    path = Path(path)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def output_directory_from_config(config_path):
    with open(config_path, "r", encoding="utf-8") as fh:
        config = yaml.load(fh, Loader=yaml.FullLoader)
    return resolve_project_path(config["out_directory"])


def instance_files(output_dir):
    return sorted(
        path for path in output_dir.glob("*.txt")
        if path.name not in TABLE_FILES and path.name != "output.txt"
    )


def main():
    parser = argparse.ArgumentParser(description="Generate validation dashboards for all FU instance files.")
    parser.add_argument(
        "--config",
        default=str(PROJECT_ROOT / "config.yaml"),
        help="Path to the converter config YAML. Defaults to ./config.yaml.",
    )
    parser.add_argument(
        "--tile-url",
        help="Optional Leaflet tile URL template passed to instance_validation_dashboard.py.",
    )
    parser.add_argument(
        "--tile-attribution",
        help="Optional HTML attribution string passed to instance_validation_dashboard.py.",
    )
    args = parser.parse_args()

    output_dir = output_directory_from_config(resolve_project_path(args.config))
    for instance_path in instance_files(output_dir):
        dashboard_path = instance_path.with_name(f"{instance_path.stem}_validation_dashboard.html")
        command = [
            sys.executable,
            str(DASHBOARD_SCRIPT),
            str(instance_path),
            "--output",
            str(dashboard_path),
        ]
        if args.tile_url:
            command.extend(["--tile-url", args.tile_url])
        if args.tile_attribution:
            command.extend(["--tile-attribution", args.tile_attribution])
        subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
