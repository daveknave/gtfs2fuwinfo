#! /usr/bin/python3.11
import argparse
import html
import io
import math
from pathlib import Path

import networkx as nx
import pandas as pd


TABLE_FILES = {
    "stoppoints": "stoppoints.txt",
    "line": "line.txt",
    "servicejourney": "servicejourney.txt",
    "deadruntime": "deadruntime.txt",
    "connections": "connections.txt",
}

DEFAULT_TILE_URL = "https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png"
DEFAULT_TILE_ATTRIBUTION = (
    '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors '
    '&copy; <a href="https://carto.com/attributions">CARTO</a>'
)

MERGED_SECTIONS = {
    "$STOPPOINTS:": "stoppoints",
    "$LINE:": "line",
    "$SERVICEJOURNEY:": "servicejourney",
    "$DEADRUNTIMES:": "deadruntime",
    "$CONNECTIONS:": "connections",
}

KPI_DESCRIPTIONS = {
    "Stops": "Number of stop points in the output instance.",
    "Lines": "Number of lines in the output instance.",
    "Service Trips": "Number of scheduled service journey rows.",
    "Dead Runtime Rows": "Number of dead runtime rows between stops.",
    "Stops Missing Outgoing Dead Runtime": "Stops without a dead runtime row starting at that stop.",
    "Stops Missing Incoming Dead Runtime": "Stops without a dead runtime row ending at that stop.",
    "Connections": "Number of connection rows in the output instance.",
    "Network Components": "Number of disconnected parts in the service trip stop network.",
    "Network Diameter": "Longest shortest path in the largest connected network component.",
    "Weighted Diameter": "Longest weighted shortest path using inverse trip frequency as distance.",
    "Avg. Clustering": "Average tendency of neighboring stops to also be connected.",
    "Network Stops": "Number of stops represented in the service trip network.",
    "Network Edges": "Number of stop-to-stop links used by service trips.",
    "Density": "Share of possible stop-to-stop links that exist in the network.",
    "Min Distance Weight": "Smallest inverse trip-frequency distance weight on a network edge.",
    "Max Distance Weight": "Largest inverse trip-frequency distance weight on a network edge.",
    "Cliques": "Number of fully connected stop groups in the network.",
    "Largest Clique": "Size of the largest fully connected stop group.",
}


def read_output_instance(instance_path):
    instance_path = Path(instance_path)
    if instance_path.is_dir():
        return read_output_directory(instance_path)
    return read_merged_instance_file(instance_path)


def read_output_directory(output_dir):
    tables = {}
    for table_name, file_name in TABLE_FILES.items():
        file_path = output_dir / file_name
        if file_path.exists():
            tables[table_name] = pd.read_csv(file_path, sep=";", dtype=str)
        else:
            tables[table_name] = pd.DataFrame()
    return tables


def read_merged_instance_file(instance_file):
    tables = {table_name: pd.DataFrame() for table_name in TABLE_FILES}
    current_section = None
    current_lines = []

    def flush_section():
        if current_section is None or not current_lines:
            return
        tables[current_section] = pd.read_csv(io.StringIO("".join(current_lines)), sep=";", dtype=str)

    with open(instance_file, "r", encoding="utf-8") as fh:
        for line in fh:
            stripped = line.strip()
            section_name = None
            section_header = None
            for marker, table_name in MERGED_SECTIONS.items():
                if stripped == marker:
                    section_name = table_name
                    section_header = ""
                    break
                if stripped.startswith(marker):
                    section_name = table_name
                    section_header = stripped[len(marker):]
                    break

            if section_name is not None:
                flush_section()
                current_section = section_name
                current_lines = []
                if section_header:
                    current_lines.append(section_header + "\n")
                continue
            if current_section is None or stripped == "*":
                continue
            current_lines.append(line)

    flush_section()
    return tables


def numeric(series):
    return pd.to_numeric(series, errors="coerce")


def parse_time_to_seconds(value):
    if pd.isna(value):
        return math.nan
    parts = str(value).split(":")
    if len(parts) != 3:
        return math.nan
    try:
        hours, minutes, seconds = [int(float(part)) for part in parts]
    except ValueError:
        return math.nan
    return hours * 3600 + minutes * 60 + seconds


def html_escape(value):
    return html.escape("" if pd.isna(value) else str(value))


def format_number(value, digits=1):
    if pd.isna(value):
        return ""
    if isinstance(value, float):
        return f"{value:,.{digits}f}"
    return f"{value:,}"


def metric_card(label, value, suffix=""):
    description = KPI_DESCRIPTIONS.get(label, "")
    escaped_description = html_escape(description)
    info = (
        f' <span class="metric-info" title="{escaped_description}" '
        f'aria-label="{escaped_description}" data-info="{escaped_description}">i</span>'
        if description else ""
    )
    return f"""
    <div class="metric">
      <div class="metric-label">{html_escape(label)}{info}</div>
      <div class="metric-value">{html_escape(value)}{html_escape(suffix)}</div>
    </div>
    """


def dataframe_to_html_table(df, columns=None, max_rows=20):
    if df is None or df.empty:
        return '<p class="empty">No data available.</p>'
    if columns is not None:
        existing_columns = [column for column in columns if column in df.columns]
        df = df[existing_columns]
    df = df.head(max_rows)
    header = "".join(f"<th>{html_escape(column)}</th>" for column in df.columns)
    rows = []
    for _, row in df.iterrows():
        cells = "".join(f"<td>{html_escape(row[column])}</td>" for column in df.columns)
        rows.append(f"<tr>{cells}</tr>")
    return f"<table><thead><tr>{header}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


def build_servicejourney_metrics(servicejourney):
    if servicejourney.empty:
        return servicejourney
    servicejourney = servicejourney.copy()
    servicejourney["Distance"] = numeric(servicejourney.get("Distance"))
    servicejourney["DepSeconds"] = servicejourney.get("DepTime", pd.Series(dtype=str)).apply(parse_time_to_seconds)
    servicejourney["ArrSeconds"] = servicejourney.get("ArrTime", pd.Series(dtype=str)).apply(parse_time_to_seconds)
    servicejourney["DurationSeconds"] = servicejourney["ArrSeconds"] - servicejourney["DepSeconds"]
    servicejourney.loc[servicejourney["DurationSeconds"] < 0, "DurationSeconds"] += 24 * 3600
    servicejourney["SpeedKmh"] = servicejourney["Distance"] / servicejourney["DurationSeconds"] * 3.6
    servicejourney.loc[servicejourney["DurationSeconds"] <= 0, "SpeedKmh"] = math.nan
    return servicejourney


def build_deadruntime_metrics(deadruntime):
    if deadruntime.empty:
        return deadruntime
    deadruntime = deadruntime.copy()
    deadruntime["Distance"] = numeric(deadruntime.get("Distance"))
    deadruntime["RunTime"] = numeric(deadruntime.get("RunTime"))
    deadruntime["SpeedKmh"] = deadruntime["Distance"] / deadruntime["RunTime"] * 3.6
    deadruntime.loc[deadruntime["RunTime"] <= 0, "SpeedKmh"] = math.nan
    return deadruntime


def describe_numeric(df, columns):
    rows = []
    for column in columns:
        if column not in df.columns:
            continue
        values = numeric(df[column]).dropna()
        if values.empty:
            continue
        rows.append(
            {
                "Metric": column,
                "Count": int(values.count()),
                "Min": round(values.min(), 2),
                "Mean": round(values.mean(), 2),
                "Median": round(values.median(), 2),
                "Max": round(values.max(), 2),
            }
        )
    return pd.DataFrame(rows)


def deadruntime_connectivity(stoppoints, deadruntime):
    columns = ["ID", "Name"]
    if stoppoints.empty or "ID" not in stoppoints.columns:
        return pd.DataFrame(columns=columns), pd.DataFrame(columns=columns)

    stops = stoppoints[columns].drop_duplicates() if "Name" in stoppoints.columns else stoppoints[["ID"]].drop_duplicates()
    if "Name" not in stops.columns:
        stops["Name"] = ""
    stop_ids = set(stops["ID"].astype(str))

    if deadruntime.empty:
        return stops.copy(), stops.copy()

    from_ids = set(deadruntime.get("FromStopID", pd.Series(dtype=str)).dropna().astype(str))
    to_ids = set(deadruntime.get("ToStopID", pd.Series(dtype=str)).dropna().astype(str))

    missing_outgoing = stops[stops["ID"].astype(str).isin(stop_ids - from_ids)].copy()
    missing_incoming = stops[stops["ID"].astype(str).isin(stop_ids - to_ids)].copy()
    return missing_outgoing, missing_incoming


def build_graph(servicejourney):
    graph = nx.Graph()
    if servicejourney.empty:
        return graph
    edge_counts = {}
    for _, row in servicejourney.iterrows():
        source = row.get("FromStopID")
        target = row.get("ToStopID")
        if pd.isna(source) or pd.isna(target):
            continue
        source = str(source)
        target = str(target)
        edge = tuple(sorted([source, target]))
        edge_counts[edge] = edge_counts.get(edge, 0) + 1

    max_count = max(edge_counts.values(), default=1)
    for (source, target), count in edge_counts.items():
        normalized_trips = count / max_count
        graph.add_edge(
            source,
            target,
            trips=count,
            normalized_trips=normalized_trips,
            weight=1 / normalized_trips,
        )
    return graph


def graph_overview(graph):
    if graph.number_of_nodes() == 0:
        return {
            "nodes": 0,
            "edges": 0,
            "components": 0,
            "diameter": "n/a",
            "weighted_diameter": "n/a",
            "avg_clustering": "n/a",
            "density": "n/a",
            "min_edge_weight": "n/a",
            "max_edge_weight": "n/a",
            "cliques": 0,
            "largest_clique": 0,
        }

    components = list(nx.connected_components(graph))
    largest_component = graph.subgraph(max(components, key=len)).copy()
    diameter = nx.diameter(largest_component) if largest_component.number_of_nodes() > 1 else 0
    weighted_diameter = 0
    if largest_component.number_of_nodes() > 1:
        weighted_paths = dict(nx.all_pairs_dijkstra_path_length(largest_component, weight="weight"))
        weighted_diameter = max(max(paths.values()) for paths in weighted_paths.values())
    cliques = list(nx.find_cliques(graph))
    return {
        "nodes": graph.number_of_nodes(),
        "edges": graph.number_of_edges(),
        "components": len(components),
        "diameter": diameter,
        "weighted_diameter": round(weighted_diameter, 4),
        "avg_clustering": round(nx.average_clustering(graph, weight="normalized_trips"), 4),
        "density": round(nx.density(graph), 4),
        "min_edge_weight": round(min((data["weight"] for _, _, data in graph.edges(data=True)), default=0), 4),
        "max_edge_weight": round(max((data["weight"] for _, _, data in graph.edges(data=True)), default=0), 4),
        "cliques": len(cliques),
        "largest_clique": max((len(clique) for clique in cliques), default=0),
    }


def centrality_table(graph, stoppoints, centrality_func, metric_name, ascending=False, **kwargs):
    if graph.number_of_nodes() == 0:
        return pd.DataFrame(columns=["StopID", "Name", metric_name])
    values = centrality_func(graph, **kwargs)
    df = pd.DataFrame({"StopID": list(values.keys()), metric_name: list(values.values())})
    df[metric_name] = df[metric_name].round(6)
    if not stoppoints.empty and "ID" in stoppoints.columns:
        names = stoppoints[["ID", "Name"]].drop_duplicates().rename(columns={"ID": "StopID"})
        df = df.merge(names, on="StopID", how="left")
    else:
        df["Name"] = ""
    return df.sort_values(metric_name, ascending=ascending)[["StopID", "Name", metric_name]]


def active_service_trips_curve(servicejourney):
    if servicejourney.empty or not {"DepSeconds", "ArrSeconds"}.issubset(servicejourney.columns):
        return pd.DataFrame(columns=["Minute", "ActiveTrips"])

    events = {}
    for _, row in servicejourney.dropna(subset=["DepSeconds", "ArrSeconds"]).iterrows():
        dep_minute = int(row["DepSeconds"] // 60)
        arr_minute = int(row["ArrSeconds"] // 60)
        if arr_minute < dep_minute:
            arr_minute += 24 * 60
        if arr_minute == dep_minute:
            continue
        events[dep_minute] = events.get(dep_minute, 0) + 1
        events[arr_minute] = events.get(arr_minute, 0) - 1

    active_trips = 0
    points = []
    for minute in sorted(events):
        active_trips += events[minute]
        points.append({"Minute": minute, "ActiveTrips": active_trips})

    return pd.DataFrame(points)


def format_minute_label(minute):
    hour = minute // 60
    minute_of_hour = minute % 60
    return f"{hour:02d}:{minute_of_hour:02d}"


def svg_active_trips_curve(points, width=920, height=300):
    if points.empty:
        return '<p class="empty">No departure and arrival times available.</p>'
    margin = {"top": 20, "right": 20, "bottom": 45, "left": 55}
    plot_width = width - margin["left"] - margin["right"]
    plot_height = height - margin["top"] - margin["bottom"]
    min_minute = int(points["Minute"].min())
    max_minute = int(points["Minute"].max())
    minute_span = max(max_minute - min_minute, 1)
    max_value = max(int(points["ActiveTrips"].max()), 1)

    def x_for(minute):
        return margin["left"] + (minute - min_minute) / minute_span * plot_width

    def y_for(value):
        return margin["top"] + plot_height - value / max_value * plot_height

    path_parts = []
    previous_x = None
    previous_y = None
    for idx, row in points.iterrows():
        x = x_for(int(row["Minute"]))
        y = y_for(int(row["ActiveTrips"]))
        if idx == points.index[0]:
            path_parts.append(f"M {x:.2f} {y:.2f}")
        else:
            path_parts.append(f"L {x:.2f} {previous_y:.2f} L {x:.2f} {y:.2f}")
        previous_x = x
        previous_y = y

    tick_count = min(8, max(2, math.ceil(minute_span / 120)))
    ticks = []
    for idx in range(tick_count + 1):
        minute = int(min_minute + minute_span * idx / tick_count)
        x = x_for(minute)
        ticks.append(
            f'<line x1="{x:.2f}" y1="{margin["top"] + plot_height}" x2="{x:.2f}" y2="{margin["top"] + plot_height + 5}" />'
            f'<text x="{x:.2f}" y="{height - 15}" text-anchor="middle">{format_minute_label(minute)}</text>'
        )

    y_ticks = []
    for idx in range(5):
        value = round(max_value * idx / 4)
        y = y_for(value)
        y_ticks.append(
            f'<line x1="{margin["left"] - 5}" y1="{y:.2f}" x2="{margin["left"]}" y2="{y:.2f}" />'
            f'<text x="{margin["left"] - 9}" y="{y + 4:.2f}" text-anchor="end">{value}</text>'
        )

    return f"""
    <svg class="chart line-chart" viewBox="0 0 {width} {height}" role="img" aria-label="Active service trips over the day">
      <line x1="{margin['left']}" y1="{margin['top'] + plot_height}" x2="{width - margin['right']}" y2="{margin['top'] + plot_height}" />
      <line x1="{margin['left']}" y1="{margin['top']}" x2="{margin['left']}" y2="{margin['top'] + plot_height}" />
      {''.join(ticks)}
      {''.join(y_ticks)}
      <text x="12" y="{margin['top'] + 14}" class="axis-label">Active trips</text>
      <text x="{width / 2}" y="{height - 2}" class="axis-label" text-anchor="middle">Time of day</text>
      <path d="{' '.join(path_parts)}" />
    </svg>
    """


def leaflet_network_map(stoppoints, servicejourney, tile_url=DEFAULT_TILE_URL, tile_attribution=DEFAULT_TILE_ATTRIBUTION):
    required_stop_columns = {"ID", "Lat", "Lon"}
    required_trip_columns = {"FromStopID", "ToStopID"}
    if stoppoints.empty or not required_stop_columns.issubset(stoppoints.columns):
        return '<p class="empty">No stop coordinates available.</p>'

    stops = stoppoints.copy()
    stops["Lat"] = numeric(stops["Lat"])
    stops["Lon"] = numeric(stops["Lon"])
    stops = stops.dropna(subset=["Lat", "Lon"])
    if stops.empty:
        return '<p class="empty">No valid stop coordinates available.</p>'

    stop_coordinates = {}
    stop_names = {}
    for _, row in stops.iterrows():
        stop_id = str(row["ID"])
        stop_coordinates[stop_id] = (float(row["Lat"]), float(row["Lon"]))
        stop_names[stop_id] = row.get("Name", "")

    edge_counts = {}
    if not servicejourney.empty and required_trip_columns.issubset(servicejourney.columns):
        for _, row in servicejourney.iterrows():
            source = str(row["FromStopID"])
            target = str(row["ToStopID"])
            if source in stop_coordinates and target in stop_coordinates and source != target:
                edge = tuple(sorted([source, target]))
                edge_counts[edge] = edge_counts.get(edge, 0) + 1

    max_edge_count = max(edge_counts.values(), default=1)
    edges_js = []
    for (source, target), count in edge_counts.items():
        source_lat, source_lon = stop_coordinates[source]
        target_lat, target_lon = stop_coordinates[target]
        normalized_trips = count / max_edge_count
        inverted_weight = 1 / normalized_trips
        stroke_width = 1.0 + 5.0 * normalized_trips
        popup = html_escape(
            f"{source} - {target}: {count} trips, normalized trips {normalized_trips:.3f}, "
            f"distance weight {inverted_weight:.3f}"
        )
        edges_js.append(
            "L.polyline("
            f"[[{source_lat:.8f}, {source_lon:.8f}], [{target_lat:.8f}, {target_lon:.8f}]], "
            f"{{color: '#546a7b', weight: {stroke_width:.2f}, opacity: 0.55}}"
            f").bindPopup({popup!r}).addTo(map);"
        )

    max_degree = max((sum(1 for edge in edge_counts if stop_id in edge) for stop_id in stop_coordinates), default=1)
    nodes_js = []
    bounds_js = []
    for stop_id, (lat, lon) in stop_coordinates.items():
        degree = sum(1 for edge in edge_counts if stop_id in edge)
        radius = 3 + 7 * degree / max(max_degree, 1)
        popup = html_escape(f"{stop_id} {stop_names.get(stop_id, '')}")
        nodes_js.append(
            "L.circleMarker("
            f"[{lat:.8f}, {lon:.8f}], "
            f"{{radius: {radius:.2f}, color: 'white', weight: 1.5, fillColor: '#d1495b', fillOpacity: 0.88}}"
            f").bindPopup({popup!r}).addTo(map);"
        )
        bounds_js.append(f"[{lat:.8f}, {lon:.8f}]")

    return f"""
    <div id="network-map"></div>
    <script>
      (function () {{
        const map = L.map('network-map');
        L.tileLayer({tile_url!r}, {{
          maxZoom: 19,
          attribution: {tile_attribution!r}
        }}).addTo(map);
        {''.join(edges_js)}
        {''.join(nodes_js)}
        const bounds = L.latLngBounds([{','.join(bounds_js)}]);
        map.fitBounds(bounds, {{padding: [18, 18]}});
      }})();
    </script>
    """


def build_dashboard(tables, title, tile_url=DEFAULT_TILE_URL, tile_attribution=DEFAULT_TILE_ATTRIBUTION):
    stoppoints = tables["stoppoints"]
    line = tables["line"]
    servicejourney = build_servicejourney_metrics(tables["servicejourney"])
    deadruntime = build_deadruntime_metrics(tables["deadruntime"])
    connections = tables["connections"]

    graph = build_graph(servicejourney)
    topology = graph_overview(graph)
    active_trips_curve = active_service_trips_curve(servicejourney)
    betweenness = centrality_table(
        graph,
        stoppoints,
        nx.betweenness_centrality,
        "Betweenness",
        weight="weight",
    )
    closeness = centrality_table(
        graph,
        stoppoints,
        nx.closeness_centrality,
        "Closeness",
        distance="weight",
    )

    service_distance_stats = describe_numeric(servicejourney, ["Distance", "DurationSeconds", "SpeedKmh"])
    deadruntime_stats = describe_numeric(deadruntime, ["Distance", "RunTime", "SpeedKmh"])
    missing_deadruntime_outgoing, missing_deadruntime_incoming = deadruntime_connectivity(stoppoints, deadruntime)
    suspicious_service = servicejourney[
        (servicejourney["DurationSeconds"].isna())
        | (servicejourney["DurationSeconds"] <= 0)
        | (servicejourney["Distance"].isna())
        | (servicejourney["SpeedKmh"] > 90)
        | (servicejourney["SpeedKmh"] < 1)
    ].copy() if not servicejourney.empty else pd.DataFrame()
    suspicious_dead = deadruntime[
        (deadruntime["RunTime"].isna())
        | (deadruntime["RunTime"] <= 0)
        | (deadruntime["Distance"].isna())
        | (deadruntime["SpeedKmh"] > 130)
        | (deadruntime["SpeedKmh"] < 1)
    ].copy() if not deadruntime.empty else pd.DataFrame()

    metrics_html = "".join(
        [
            metric_card("Stops", format_number(len(stoppoints), 0)),
            metric_card("Lines", format_number(len(line), 0)),
            metric_card("Service Trips", format_number(len(servicejourney), 0)),
            metric_card("Dead Runtime Rows", format_number(len(deadruntime), 0)),
            metric_card("Stops Missing Outgoing Dead Runtime", format_number(len(missing_deadruntime_outgoing), 0)),
            metric_card("Stops Missing Incoming Dead Runtime", format_number(len(missing_deadruntime_incoming), 0)),
            metric_card("Connections", format_number(len(connections), 0)),
            metric_card("Network Components", topology["components"]),
            metric_card("Network Diameter", topology["diameter"]),
            metric_card("Weighted Diameter", topology["weighted_diameter"]),
            metric_card("Avg. Clustering", topology["avg_clustering"]),
        ]
    )

    topology_metrics_html = "".join(
        [
            metric_card("Network Stops", topology["nodes"]),
            metric_card("Network Edges", topology["edges"]),
            metric_card("Density", topology["density"]),
            metric_card("Min Distance Weight", topology["min_edge_weight"]),
            metric_card("Max Distance Weight", topology["max_edge_weight"]),
            metric_card("Cliques", topology["cliques"]),
            metric_card("Largest Clique", topology["largest_clique"]),
        ]
    )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{html_escape(title)} - Instance Validation Dashboard</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <style>
    body {{ margin: 0; font-family: Arial, sans-serif; color: #202124; background: #f7f8fa; }}
    header {{ padding: 28px 36px; background: #17324d; color: white; }}
    h1 {{ margin: 0; font-size: 28px; }}
    h2 {{ margin: 0 0 16px; font-size: 20px; }}
    h3 {{ margin: 18px 0 10px; font-size: 15px; }}
    main {{ padding: 24px 36px 42px; }}
    section {{ margin-bottom: 24px; padding: 20px; background: white; border: 1px solid #d9dee7; border-radius: 6px; }}
    .metrics {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 12px; }}
    .metric {{ padding: 14px; border: 1px solid #d9dee7; border-radius: 6px; background: #fbfcfe; }}
    .metric-label {{ color: #5f6b7a; font-size: 12px; text-transform: uppercase; }}
    .metric-info {{ position: relative; display: inline-block; width: 14px; height: 14px; margin-left: 4px; border-radius: 50%; background: #d9dee7; color: #17324d; font-size: 10px; line-height: 14px; text-align: center; text-transform: none; cursor: help; }}
    .metric-info:hover::after {{ content: attr(data-info); position: absolute; left: 18px; top: 50%; transform: translateY(-50%); z-index: 10; width: 220px; padding: 7px 9px; border-radius: 4px; background: #202124; color: white; font-size: 12px; line-height: 1.35; text-align: left; }}
    .metric-value {{ margin-top: 6px; font-size: 24px; font-weight: 700; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ padding: 8px 10px; border-bottom: 1px solid #e6e9ef; text-align: left; }}
    th {{ background: #eef2f7; font-weight: 700; }}
    .grid-2 {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(340px, 1fr)); gap: 18px; }}
    .section-row {{ margin-top: 18px; }}
    .visual-row {{ display: grid; grid-template-columns: minmax(360px, 0.9fr) minmax(460px, 1.1fr); gap: 18px; align-items: stretch; }}
    .visual-panel {{ min-width: 0; }}
    .chart {{ width: 100%; height: auto; }}
    .chart line {{ stroke: #697386; stroke-width: 1; }}
    .chart text {{ fill: #4d5968; font-size: 11px; }}
    .line-chart path {{ fill: none; stroke: #2d7dd2; stroke-width: 2.5; }}
    .axis-label {{ font-weight: 700; }}
    #network-map {{ height: 360px; min-height: 360px; border: 1px solid #c6ced8; border-radius: 6px; }}
    .empty {{ color: #6f7a88; font-style: italic; }}
    @media (max-width: 980px) {{
      .visual-row {{ grid-template-columns: 1fr; }}
      #network-map {{ height: 420px; }}
    }}
  </style>
</head>
<body>
  <header>
    <h1>Instance Validation Dashboard</h1>
    <div>{html_escape(title)}</div>
  </header>
  <main>
    <section>
      <h2>Descriptive Statistics</h2>
      <div class="metrics">{metrics_html}</div>
    </section>

    <section>
      <div class="visual-row">
        <div class="visual-panel">
          <h2>Service Trips Over The Day</h2>
          {svg_active_trips_curve(active_trips_curve)}
        </div>
        <div class="visual-panel">
          <h2>Stop And Service Trip Network Map</h2>
          {leaflet_network_map(stoppoints, servicejourney, tile_url, tile_attribution)}
        </div>
      </div>
    </section>

    <section>
      <h2>Topological Analysis</h2>
      <div class="metrics">{topology_metrics_html}</div>
      <div class="grid-2">
        <div>
          <h3>Top 10 Betweenness Centrality</h3>
          {dataframe_to_html_table(betweenness.head(10), max_rows=10)}
        </div>
        <div>
          <h3>Top 10 Closeness Centrality</h3>
          {dataframe_to_html_table(closeness.head(10), max_rows=10)}
        </div>
        <div>
          <h3>Bottom 10 Betweenness Centrality</h3>
          {dataframe_to_html_table(betweenness.tail(10), max_rows=10)}
        </div>
        <div>
          <h3>Bottom 10 Closeness Centrality</h3>
          {dataframe_to_html_table(closeness.tail(10), max_rows=10)}
        </div>
      </div>
    </section>

    <section>
      <h2>Distance, Duration And Speed Validation</h2>
      <div class="grid-2">
        <div>
          <h3>Service Trip Metrics</h3>
          {dataframe_to_html_table(service_distance_stats)}
        </div>
        <div>
          <h3>Dead Runtime Metrics</h3>
          {dataframe_to_html_table(deadruntime_stats)}
        </div>
      </div>
      <div class="grid-2 section-row">
        <div>
          <h3>Stops Missing Outgoing Dead Runtime</h3>
          {dataframe_to_html_table(missing_deadruntime_outgoing, max_rows=25)}
        </div>
        <div>
          <h3>Stops Missing Incoming Dead Runtime</h3>
          {dataframe_to_html_table(missing_deadruntime_incoming, max_rows=25)}
        </div>
      </div>
      <div class="grid-2 section-row">
        <div>
          <h3>Suspicious Service Trips</h3>
          {dataframe_to_html_table(suspicious_service, ["ID", "LineID", "FromStopID", "ToStopID", "Distance", "DurationSeconds", "SpeedKmh"], max_rows=25)}
        </div>
        <div>
          <h3>Suspicious Dead Runtimes</h3>
          {dataframe_to_html_table(suspicious_dead, ["FromStopID", "ToStopID", "Distance", "RunTime", "SpeedKmh"], max_rows=25)}
        </div>
      </div>
    </section>
  </main>
</body>
</html>
"""


def default_dashboard_path(instance_path):
    instance_path = Path(instance_path)
    if instance_path.is_dir():
        instance_files = sorted(
            (
                path for path in instance_path.glob("*.txt")
                if path.name not in TABLE_FILES.values() and path.name != "output.txt"
            ),
            key=lambda path: path.stat().st_mtime,
        )
        if instance_files:
            return instance_path / f"{instance_files[-1].stem}_validation_dashboard.html"
        return instance_path / f"{instance_path.name}_validation_dashboard.html"
    return instance_path.with_name(f"{instance_path.stem}_validation_dashboard.html")


def main():
    parser = argparse.ArgumentParser(description="Generate an HTML validation dashboard for a GTFS2FU output instance.")
    parser.add_argument(
        "instance",
        nargs="?",
        default="output",
        help="Output directory or merged FU instance file. Defaults to ./output.",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Dashboard HTML path. Defaults to validation_dashboard.html beside the instance.",
    )
    parser.add_argument(
        "--tile-url",
        default=DEFAULT_TILE_URL,
        help=(
            "Leaflet tile URL template. Defaults to an OSM-derived CARTO layer to avoid "
            "OSMF tile.openstreetmap.org blocks when opening local HTML files."
        ),
    )
    parser.add_argument(
        "--tile-attribution",
        default=DEFAULT_TILE_ATTRIBUTION,
        help="HTML attribution string for the selected tile layer.",
    )
    args = parser.parse_args()

    instance_path = Path(args.instance)
    output_path = Path(args.output) if args.output else default_dashboard_path(instance_path)
    tables = read_output_instance(instance_path)
    dashboard = build_dashboard(tables, str(instance_path), args.tile_url, args.tile_attribution)
    output_path.write_text(dashboard, encoding="utf-8")
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
