#! /usr/bin/python3.11
import argparse
import multiprocessing as mp
import sys
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from GTFS2FU.converter import Gtfs2FuwInfoConverter
from GTFS2FU.instance_validation_dashboard import (
    DEFAULT_TILE_ATTRIBUTION,
    DEFAULT_TILE_URL,
    build_dashboard,
    default_dashboard_path,
    read_output_instance,
)
from GTFS2FU.merge_output_data import merge_output_data


def resolve_project_path(path):
    path = Path(path)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def load_config(config_path):
    with open(config_path, "r") as fh:
        config = yaml.load(fh, Loader=yaml.FullLoader)

    config["in_directory"] = str(resolve_project_path(config["in_directory"]))
    config["out_directory"] = str(resolve_project_path(config["out_directory"]))
    return config


def write_validation_dashboard(instance_path, output_path, tile_url, tile_attribution):
    tables = read_output_instance(instance_path)
    dashboard = build_dashboard(tables, str(instance_path), tile_url, tile_attribution)
    output_path.write_text(dashboard, encoding="utf-8")
    print(f"Wrote {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Run GTFS2FU conversion and generate a validation dashboard.")
    parser.add_argument(
        "--config",
        default=str(PROJECT_ROOT / "config.yaml"),
        help="Path to the converter config YAML. Defaults to ./config.yaml.",
    )
    parser.add_argument(
        "--dashboard-output",
        help="Dashboard HTML path. Defaults to validation_dashboard.html in the configured output directory.",
    )
    parser.add_argument(
        "--tile-url",
        default=DEFAULT_TILE_URL,
        help="Leaflet tile URL template for the validation dashboard.",
    )
    parser.add_argument(
        "--tile-attribution",
        default=DEFAULT_TILE_ATTRIBUTION,
        help="HTML attribution string for the selected tile layer.",
    )
    args = parser.parse_args()

    mp.freeze_support()
    config = load_config(resolve_project_path(args.config))

    Gtfs2FuwInfoConverter(config).run()
    merge_output_data(config)

    output_dir = Path(config["out_directory"])
    dashboard_output = Path(args.dashboard_output) if args.dashboard_output else default_dashboard_path(output_dir)
    write_validation_dashboard(output_dir, dashboard_output, args.tile_url, args.tile_attribution)


if __name__ == "__main__":
    main()
