import datetime
import math
import os
from itertools import combinations

import networkx as nx
import numpy as np
import pandas as pd
from pip._internal import locations

from GTFS2FU import utility_functions as uf, pargroupby, routing as drt


class Gtfs2FuwInfoConverter:
    def __init__(self, config, osrm_tab_req_limit=100):
        self.config = config
        self.data_dir = config["in_directory"]
        self.out_dir = config["out_directory"]
        self.osrm_tab_req_limit = osrm_tab_req_limit

        self.input_tables = {}
        self.depots = None
        self.stoppoints_translation = {}

    def run(self):
        self.load_input()
        tr_df = self.prepare_valid_trips()
        stp_df = self.prepare_stops()
        str_df = self.prepare_stop_times(tr_df, stp_df)
        servicejourney = self.build_servicejourney(str_df)
        stoppoints = self.build_stoppoints(str_df, servicejourney)
        line = self.build_line(str_df)
        deadruntimes = self.build_deadruntimes(stoppoints)
        connections = self.build_connections(stoppoints)

        self.write_outputs(stoppoints, servicejourney, line, deadruntimes, connections)
        return stoppoints, servicejourney, line, connections, deadruntimes

    def load_input(self):
        for file_name in os.listdir(self.data_dir):
            if ".txt" not in file_name:
                continue
            self.input_tables[file_name] = pd.read_csv(
                os.path.join(self.data_dir, file_name),
                delimiter=",",
                decimal=".",
                quotechar='"',
            )

        self.depots = pd.read_csv(os.path.join(self.data_dir, "vbb_depots.csv"), sep=";", decimal=",")

    def prepare_valid_trips(self):
        routes = self.input_tables["routes.txt"]
        tr_df = self.input_tables["trips.txt"].merge(routes, on="route_id")
        tr_df = tr_df[
            (tr_df["agency_id"] == self.config["agency"])
            & (tr_df["route_type"] == self.config["veh_type"])
        ]

        cal = self.input_tables["calendar.txt"].copy()
        cal["start_date"] = cal["start_date"].apply(lambda x: datetime.datetime.strptime(str(x), "%Y%m%d"))
        cal["end_date"] = cal["end_date"].apply(lambda x: datetime.datetime.strptime(str(x), "%Y%m%d"))

        cal_exceptions = self.input_tables["calendar_dates.txt"].copy()
        cal_exceptions["date"] = cal_exceptions["date"].apply(
            lambda x: datetime.datetime.strptime(str(x), "%Y%m%d")
        )

        pit_dt = datetime.datetime.strptime(self.config["point_in_time"], "%Y-%m-%d")

        cal_exceptions = cal_exceptions[cal_exceptions["date"] == pit_dt.strftime("%Y%m%d")]
        cal = cal[
            (cal[pit_dt.strftime("%A").lower()] == 1)
            & (cal["start_date"] <= pit_dt)
            & (pit_dt <= cal["end_date"])
        ]

        d1 = tr_df.merge(cal, how="inner", on="service_id").set_index("trip_id")
        d2 = tr_df.merge(cal_exceptions, how="inner", on="service_id").set_index("trip_id")

        tr_df = tr_df.set_index("trip_id")
        tr_df["valid"] = False
        tr_df.loc[d1.index, "valid"] = True
        tr_df.loc[d2[d2["exception_type"] == 1].index, "valid"] = True
        tr_df.loc[d2[d2["exception_type"] == 2].index, "valid"] = False

        tr_df = tr_df.loc[tr_df['valid'],:]

        if self.config["debug"] > 0:
            # tr_df = tr_df.head(100)
            tr_df = tr_df.loc[tr_df['route_id'] == '23984_700']
        return tr_df

    def prepare_stops(self):
        stp_df = self.input_tables["stops.txt"].copy()
        stp_df.loc[:, "ID"] = stp_df.apply(
            lambda a: a["stop_id"] if ":" in a["stop_id"] else a["parent_station"],
            axis=1,
        )
        stp_df.loc[np.logical_not(stp_df["ID"].isna()), "ID"] = stp_df.loc[
            np.logical_not(stp_df["ID"].isna()),
            "ID",
        ].apply(lambda a: a.split(":")[2])
        stp_df.loc[stp_df["ID"].isna(), "ID"] = stp_df.loc[stp_df["ID"].isna(), "stop_id"]

        tmp_stp_df = stp_df.set_index("ID")
        tmp_stp_df.loc[:, ["stop_lat", "stop_lon"]] = (
            stp_df.groupby("ID")
            .apply(lambda a: uf.find_center_coordinates(a))
            .reset_index()
            .set_index("ID")[["stop_lat", "stop_lon"]]
        )

        stp_df = tmp_stp_df.reset_index()

        tmp_dict = stp_df[["stop_id", "ID"]].to_dict(orient="list")
        self.stoppoints_translation = dict(zip(tmp_dict["stop_id"], tmp_dict["ID"]))

        return stp_df

    def prepare_stop_times(self, tr_df, stp_df):
        ts_df = tr_df[tr_df["valid"]].merge(self.input_tables["stop_times.txt"], on="trip_id", how="left")

        shapes_df = self.input_tables["shapes.txt"].set_index("shape_id")
        shapes_df = shapes_df.loc[shapes_df.index.intersection(ts_df["shape_id"].dropna().unique())]
        shape_distances = self.calculate_shape_distances(shapes_df)

        str_df = ts_df.merge(stp_df, how="left", on="stop_id").drop_duplicates()
        str_df = str_df.sort_values(["trip_id", "stop_sequence"])
        return str_df.merge(shape_distances, how="left", on="shape_id")

    def calculate_shape_distances(self, shapes_df):
        if shapes_df.empty or not np.any(shapes_df.apply(lambda d: uf.is_plausibly_germany(d['shape_pt_lat'], d['shape_pt_lon']),axis=1)):
            return pd.DataFrame(columns=["shape_id", "distance"])
        if self.config['parallel'] == 0:
            shape_distances = shapes_df.groupby("shape_id", as_index=True).apply(lambda a: uf.shape2distance(a))
        else:
            shape_distances = pargroupby.do(
                gr=shapes_df.groupby("shape_id", as_index=True),
                func=uf.shape2distance,
                name="shape2distance",
                ncores=8,
            )

        if isinstance(shape_distances, pd.Series):
            shape_distances = pd.DataFrame(shape_distances.rename("distance"))
        else:
            shape_distances = shape_distances.rename(columns={0: 'shape_id', 1:"distance"})

        if np.any(shape_distances['distance'].isna()):
            print('Problem')
        return shape_distances
        # return shape_distances.rename(columns={0: "shape_id", 1: "distance"})

    def build_servicejourney(self, str_df):
        if self.config['parallel'] == 0:
            sjdf = str_df.groupby("trip_id", as_index=False).apply(lambda d: uf.to_edges(d, index=d.name))
        else:
            sjdf = pargroupby.do(
                gr=str_df.groupby("trip_id"),
                func=uf.to_edges,
                name="to_edges",
                ncores=8,
            )

        missing_distance_trip_ids = sjdf.loc[sjdf["distance"].isna(), "trip_id"]

        if not missing_distance_trip_ids.empty:
            if self.config['parallel'] == 0:
                stopsequence_distances = (str_df[str_df["trip_id"].isin(missing_distance_trip_ids)].groupby("trip_id", as_index=True)
                                          .apply(lambda d: uf.stopsequence2distance(d)).rename("distance"))
            else:
                stopsequence_distances = pargroupby.do(
                    gr=str_df[str_df["trip_id"].isin(missing_distance_trip_ids)].groupby("trip_id"),
                    func=uf.stopsequence2distance,
                    name="stopsequence2distance",
                    ncores=8
                ).rename(columns={0: "trip_id", 1: "distance"})
            sjdf.set_index("trip_id", inplace=True)
            sjdf.loc[sjdf["distance"].isna(), 'distance'] = stopsequence_distances

        sjdf["min_dwell"] = 0
        sjdf["min_ahead"] = 0
        sjdf["backshift"] = 0
        sjdf["forwardshift"] = 0

        sjdf.reset_index(drop=False, inplace=True)

        servicejourney = sjdf.rename(columns={
                "trip_id": "ID",
                "route_id": "LineID",
                "from": "FromStopID",
                "to": "ToStopID",
                "dep": "DepTime",
                "arr": "ArrTime",
                "min_ahead": "MinAheadTime",
                "min_dwell": "MinLayoverTime",
                "vehicle_type": "VehTypeGroupID",
                "backshift": "MaxShiftBackwardSeconds",
                "forwardshift": "MaxShiftForwardSeconds",
                "distance": "Distance",
            }
        )
        servicejourney.drop(columns=["service_id"], inplace=True)
        servicejourney[["FromStopID", "ToStopID"]] = servicejourney[["FromStopID", "ToStopID"]].replace(
            self.stoppoints_translation
        )
        return servicejourney

    def build_stoppoints(self, str_df, servicejourney):
        stoppoints = str_df[["ID", "stop_code", "stop_name", "stop_lat", "stop_lon"]].drop_duplicates()
        stoppoints = stoppoints.rename(
            columns={
                "stop_id": "ID",
                "stop_code": "Code",
                "stop_name": "Name",
                "stop_lat": "Lat",
                "stop_lon": "Lon",
            }
        )
        stoppoints["IsChargingStation"] = 0
        stoppoints["IsDepot"] = 0

        depots = self.depots.loc[
            self.depots["agency_id"] == self.config["agency"],
            ["depot_id", "depot_name", "lat_6dp", "lon_6dp"],
        ]
        depots = depots.rename(
            columns={
                "depot_id": "ID",
                "depot_name": "Name",
                "lat_6dp": "Lat",
                "lon_6dp": "Lon",
            }
        )
        depots[["Lat", "Lon"]] = depots[["Lat", "Lon"]].astype(float)
        depots.loc[:, "Code"] = depots["ID"]
        depots.loc[:, "IsChargingStation"] = 1
        depots.loc[:, "IsDepot"] = 1

        stoppoints = pd.concat([stoppoints, depots], axis=0, ignore_index=True)
        stoppoints = self.mark_charging_stations(stoppoints, servicejourney)
        return stoppoints[
            (stoppoints["ID"].isin(servicejourney["FromStopID"]))
            | (stoppoints["ID"].isin(servicejourney["ToStopID"]))
            | (stoppoints["IsDepot"] == 1)
        ]

    def mark_charging_stations(self, stoppoints, servicejourney):
        tmp_sj_df = servicejourney.rename(
            columns={
                "FromStopID": "source",
                "ToStopID": "target",
            }
        )
        edgelist = (
            tmp_sj_df[["source", "target", "ID"]]
            .groupby(["source", "target"], as_index=True)
            .apply("count")
            .reset_index()
            .rename(columns={"ID": "weight"})
        )
        graph = nx.from_pandas_edgelist(edgelist, source="source", target="target", edge_attr="weight")

        nx.set_node_attributes(graph, nx.centrality.betweenness_centrality(graph), "centrality")
        node_df = pd.DataFrame({"centrality": nx.get_node_attributes(graph, name="centrality")})

        charging_stations = node_df.sort_values(by=["centrality"], ascending=False).head(
            math.ceil(node_df.shape[0] * 0.15)
        ).reset_index()

        stoppoints.loc[np.isin(stoppoints["ID"], charging_stations["index"].astype(str)), "IsChargingStation"] = 1
        return stoppoints

    def build_line(self, str_df):
        line = str_df[["route_id", "route_short_name"]].drop_duplicates()
        line = line.rename(
            columns={
                "route_id": "ID",
                "route_short_name": "Code",
            }
        )
        line["Name"] = line["Code"]
        return line

    def build_deadruntimes(self, stoppoints):
        stoppoints = stoppoints.copy()

        locations = pd.DataFrame(columns=["start", "ID"])

        locations['start'] = stoppoints.apply(lambda x: ",".join([str(x["Lon"]), str(x["Lat"])]), axis=1)
        locations['ID'] = stoppoints['ID']

        real_routes = pd.DataFrame()
        parts = math.ceil(locations.shape[0] / (self.osrm_tab_req_limit * 0.5))
        if parts < 2:
            real_routes = pd.concat(
                [
                    real_routes,
                    drt.run_matrix_request(locations),
                ],
                axis=0, ignore_index=True
            )
        else:
            for part in combinations([p for p in range(parts)], 2):
                ir1 = (
                    part[0] * int(self.osrm_tab_req_limit * 0.5),
                    min(locations.shape[0], (part[0] + 1) * int(self.osrm_tab_req_limit * 0.5)),
                )
                ir2 = (
                    part[1] * int(self.osrm_tab_req_limit * 0.5),
                    min(locations.shape[0], (part[1] + 1) * int(self.osrm_tab_req_limit * 0.5)),
                )

                print(ir1, ir2)
                real_routes = pd.concat(
                    [
                        real_routes,
                        drt.run_matrix_request(locations.iloc[np.r_[ir1[0]:ir1[1], ir2[0]:ir2[1]]]),
                    ],
                    axis=0,
                    ignore_index=True
                )

        deadruntimes = real_routes
        deadruntimes = deadruntimes.rename(
            columns={
                "start": "FromStopID",
                "dest": "ToStopID",
                "distances": "Distance",
                "durations": "RunTime",
            }
        )
        deadruntimes["FromTime"] = 0
        deadruntimes["ToTime"] = 3600 * 32

        deadruntimes[["FromStopID", "ToStopID"]] = deadruntimes[["FromStopID", "ToStopID"]].replace(
            self.stoppoints_translation
        )
        return deadruntimes[
            ["FromStopID", "ToStopID", "FromTime", "ToTime", "Distance", "RunTime"]
        ].drop_duplicates()

    def build_connections(self, stoppoints):
        connections = self.input_tables["transfers.txt"].copy()
        connections = connections[
            (connections["from_stop_id"].isin(stoppoints["ID"]))
            & (connections["to_stop_id"].isin(stoppoints["ID"]))
            & (connections["transfer_type"] == 1)
        ]

        connections = connections.rename(
            columns={
                "from_stop_id": "FromStopID",
                "to_stop_id": "ToStopID",
                "from_route_id": "FromLineID",
                "to_route_id": "ToLineID",
                "min_transfer_time": "MinTransferTime",
            }
        )
        return connections[["FromStopID", "ToStopID", "FromLineID", "ToLineID", "MinTransferTime"]]

    def write_outputs(self, stoppoints, servicejourney, line, deadruntimes, connections):
        servicejourney.to_csv(os.path.join(self.out_dir, "servicejourney.txt"), index=False, sep=";")
        stoppoints.to_csv(os.path.join(self.out_dir, "stoppoints.txt"), index=False, sep=";")
        line.to_csv(os.path.join(self.out_dir, "line.txt"), index=False, sep=";")
        deadruntimes.to_csv(os.path.join(self.out_dir, "deadruntime.txt"), index=False, sep=";")
        connections.to_csv(os.path.join(self.out_dir, "connections.txt"), index=False, sep=";")
