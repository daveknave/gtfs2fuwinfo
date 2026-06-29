#! /usr/bin/python3.11
import math
import os
import sys
from itertools import combinations
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import datetime
import importlib
import multiprocessing as mp

import networkx as nx
import numpy as np
import pandas as pd
import yaml

### Custom functions
import pargroupby
importlib.reload(pargroupby)

import routing as drt
importlib.reload(drt)

import utility_functions as uf
importlib.reload(uf)

import merge_output_data as mod
importlib.reload(mod)

osrm_tab_req_limit = 100


def resolve_project_path(path):
    path = Path(path)
    if path.is_absolute():
        return str(path)
    return str(PROJECT_ROOT / path)


def normalize_config_paths(config):
    config = config.copy()
    config["in_directory"] = resolve_project_path(config["in_directory"])
    config["out_directory"] = resolve_project_path(config["out_directory"])
    return config


def load_config(config_path=None):
    if config_path is None:
        config_path = PROJECT_ROOT / "config.yaml"

    with open(config_path, "r") as fh:
        return normalize_config_paths(yaml.load(fh, Loader=yaml.FullLoader))


def do_the_magic(config):
    config = normalize_config_paths(config)

    ### Read GTFS data files
    data_dir = config['in_directory']

    input_tables = {}
    for f in os.listdir(data_dir):
        if not '.txt' in f: continue
        tmp_df = pd.read_csv(os.path.join(data_dir, f), delimiter=',', decimal='.', quotechar='"')
        input_tables[f] = tmp_df

    depots = pd.read_csv(os.path.join(data_dir, 'vbb_depots.csv'), sep=";", decimal=',')

    routes = input_tables['routes.txt']

    ### Prepare data
    tr_df = input_tables['trips.txt'].merge(routes, on='route_id')
    tr_df = tr_df[(tr_df['agency_id'] == config['agency']) & (tr_df['route_type'] == config['veh_type'])]

    ### Interprete calendar
    cal = input_tables['calendar.txt'].copy()
    cal['start_date'] = cal['start_date'].apply(lambda x: datetime.datetime.strptime(str(x), '%Y%m%d'))
    cal['end_date'] = cal['end_date'].apply(lambda x: datetime.datetime.strptime(str(x), '%Y%m%d'))

    cal_exceptions = input_tables['calendar_dates.txt'].copy()
    cal_exceptions['date'] = cal_exceptions['date'].apply(lambda x: datetime.datetime.strptime(str(x), '%Y%m%d'))

    pit_dt = datetime.datetime.strptime(config['point_in_time'], '%Y-%m-%d')

    cal_exceptions = cal_exceptions[cal_exceptions['date'] == pit_dt.strftime('%Y%m%d')]
    cal = cal[(cal[pit_dt.strftime('%A').lower()] == 1) & (cal['start_date'] <= pit_dt) & (pit_dt <= cal['end_date'])]

    d1 = tr_df.merge(cal, how='inner', on='service_id').set_index('trip_id')
    d2 = tr_df.merge(cal_exceptions, how='inner', on='service_id').set_index('trip_id')

    tr_df = tr_df.set_index('trip_id')
    tr_df['valid'] = False
    tr_df.loc[d1.index, 'valid'] = True
    tr_df.loc[d2[d2['exception_type'] == 1].index, 'valid'] = True
    tr_df.loc[d2[d2['exception_type'] == 2].index, 'valid'] = False


    if config['debug'] > 0:
        tr_df = tr_df.head(50)

    ### Select bus services
    ts_df = tr_df[tr_df['valid']].merge(input_tables['stop_times.txt'], on='trip_id', how='left')

    stp_df = input_tables['stops.txt']
    stp_df.loc[:, 'ID'] = stp_df.apply(lambda a: a['stop_id'] if ':' in a['stop_id'] else a['parent_station'], axis=1)
    stp_df.loc[np.logical_not(stp_df['ID'].isna()), 'ID'] = stp_df.loc[np.logical_not(stp_df['ID'].isna()), 'ID'].apply(
        lambda a: a.split(':')[2])
    stp_df.loc[stp_df['ID'].isna(), 'ID'] = stp_df.loc[stp_df['ID'].isna(), 'stop_id']

    tmp_stp_df = stp_df.set_index('ID')

    ### Find center coordinate of stops with identical ID
    tmp_stp_df.loc[:, ['stop_lat', 'stop_lon']] = \
    stp_df.groupby('ID').apply(lambda a: uf.find_center_coordinates(a)).reset_index().set_index('ID')[
        ['stop_lat', 'stop_lon']]
    stp_df = tmp_stp_df.reset_index()

    ### Create stoppoint ID translation table
    tmp_dict = stp_df[['stop_id', 'ID']].to_dict(orient='list')
    stoppoints_translation = dict(zip(tmp_dict['stop_id'], tmp_dict['ID']))

    shapes_df = input_tables['shapes.txt'].set_index('shape_id')
    shapes_df = shapes_df.loc[ts_df['shape_id'].unique()]


    ### Calculate Trip Distances
    shape_distances = pargroupby.do(gr=shapes_df.groupby('shape_id', as_index=True),
                                    func=uf.shape2distance,
                                    name='shape2distance',
                                    ncores=32
                                    )

    shape_distances = shape_distances.rename(columns={0: 'distance'})

    str_df = ts_df.merge(stp_df, how="left", on='stop_id').drop_duplicates().sort_values(['trip_id', 'stop_sequence'])

    str_df = str_df.merge(shape_distances.reset_index().rename(columns={'index':'shape_id'}), how="left", on='shape_id')

    ### Convert trip list to trips
    sjdf = pargroupby.do(gr=str_df.groupby('trip_id'),
                         func=uf.to_edges,
                         name='to_edges',
                         ncores=32
                         )

    # sjdf = pargroupby.do(gr=str_df[str_df.columns].groupby('trip_id'),
    #                      func=to_edge,
    #                      name='2edges',
    #                      ncores=8)

    ### Generate Output-Data

    ### $STOPPOINTS
    ### $STOPPOINT:ID;Code;Name;VehCapacityForCharging
    ### $STOPPOINT:ID;Code;Name;IsChargingStation;IsDepot
    stoppoints = str_df[['ID', 'stop_code', 'stop_name', 'stop_lat', 'stop_lon']].drop_duplicates()

    stoppoints = stoppoints.rename(columns={
        'stop_id': 'ID',
        'stop_code': 'Code',
        'stop_name': 'Name',
        'stop_lat': 'Lat',
        'stop_lon': 'Lon',
    })
    stoppoints['IsChargingStation'] = 0
    stoppoints['IsDepot'] = 0

    depots = depots.loc[
        depots['agency_id'] == config['agency'],
        ['depot_id', 'depot_name', 'lat_6dp', 'lon_6dp']
    ]
    depots = depots.rename(columns={
        'depot_id': 'ID',
        'depot_name': 'Name',
        'lat_6dp': 'Lat',
        'lon_6dp': 'Lon',
    })
    depots[['Lat', 'Lon']] = depots[['Lat', 'Lon']].astype(float)

    depots.loc[:, 'Code'] = depots['ID']

    depots.loc[:, 'IsChargingStation'] = 1
    depots.loc[:, 'IsDepot'] = 1

    stoppoints = pd.concat([stoppoints, depots], axis=0, ignore_index=True)

    ### $SERVICEJOURNEY
    ### $SERVICEJOURNEY:ID;LineID;FromStopID;ToStopID;DepTime;ArrTime;MinAheadTime;MinLayoverTime;VehTypeGroupID;MaxShiftBackwardSeconds;MaxShiftForwardSeconds;Distance
    sjdf['min_dwell'] = 0
    sjdf['min_ahead'] = 0
    sjdf['backshift'] = 0
    sjdf['forwardshift'] = 0

    servicejourney = sjdf.rename(columns={
        'trip_id': 'ID',
        'route_id': 'LineID',
        'from': 'FromStopID',
        'to': 'ToStopID',
        'dep': 'DepTime',
        'arr': 'ArrTime',
        'min_ahead': 'MinAheadTime',
        'min_dwell': 'MinLayoverTime',
        'vehicle_type': 'VehTypeGroupID',
        'backshift': 'MaxShiftBackwardSeconds',
        'forwardshift': 'MaxShiftForwardSeconds',
        'distance': 'Distance',
    })
    servicejourney.drop(columns=['service_id'], inplace=True)
    servicejourney[['FromStopID', 'ToStopID']] = servicejourney[['FromStopID', 'ToStopID']].replace(
        stoppoints_translation)

    servicejourney.to_csv(os.path.join(config['out_directory'], 'servicejourney.txt'), index=False, sep=';')

    ### Charging Locations with high centrallity
    tmp_sj_df = servicejourney.rename(columns={
        'FromStopID': 'source',
        'ToStopID': 'target',
    })
    edgelist = tmp_sj_df[['source', 'target', 'ID']].groupby(['source', 'target'], as_index=True).apply(
        'count').reset_index().rename(columns={'ID': 'weight'})
    G = nx.from_pandas_edgelist(edgelist, source='source', target='target', edge_attr='weight')

    nx.set_node_attributes(G, nx.centrality.betweenness_centrality(G), 'centrality')
    node_df = pd.DataFrame({'centrality': nx.get_node_attributes(G, name='centrality')})

    charging_stations = node_df.sort_values(by=['centrality'], ascending=False).head(
        math.ceil(node_df.shape[0] * 0.15)).reset_index()

    stoppoints.loc[np.isin(stoppoints['ID'], charging_stations['index'].astype(str)), 'IsChargingStation'] = 1

    stoppoints = stoppoints[
        (stoppoints['ID'].isin(servicejourney['FromStopID'])) | (stoppoints['ID'].isin(servicejourney['ToStopID'])) | (
                    stoppoints['IsDepot'] == 1)]

    ### Output STOPPOINT
    stoppoints.to_csv(os.path.join(config['out_directory'], 'stoppoints.txt'), index=False, sep=';')

    ### $LINE
    ### $LINE:ID;Code;Name
    line = str_df[['route_id', 'route_short_name']].drop_duplicates()
    line = line.rename(columns={
        'route_id': 'ID',
        'route_short_name': 'Code',
    })
    line['Name'] = line['Code']
    line.to_csv(os.path.join(config['out_directory'], 'line.txt'), index=False, sep=';')

    # %%
    ### $DEADRUNTIME
    ### $DEADRUNTIME:FromStopID;ToStopID;FromTime;ToTime;Distance;RunTime

    # Create Deadhead matrix
    stoppoints['key'] = 1
    deadruntimes = stoppoints.merge(stoppoints, on="key")

    locations = pd.DataFrame(columns=['start', 'ID'])
    locations['start'] = (deadruntimes.loc[deadruntimes['ID_x'] != deadruntimes['ID_y'], :].apply(
        lambda x: ','.join([str(x['Lon_x']), str(x['Lat_x'])]), axis=1)).unique()
    locations['ID'] = deadruntimes['ID_x']

    real_routes = pd.DataFrame()

    parts = math.ceil(locations.shape[0] / (osrm_tab_req_limit*0.5))

    combs = combinations([p for p in range(parts)], 2)

    for part in combs:
        ir1 = (part[0] * int(osrm_tab_req_limit*0.5),min(locations.shape[0],
                                                             (part[0] + 1) * int(osrm_tab_req_limit*0.5)))
        ir2 = (part[1] * int(osrm_tab_req_limit*0.5),min(locations.shape[0],
                                                             (part[1] + 1) * int(osrm_tab_req_limit*0.5)))

        print(ir1, ir2)

        real_routes = pd.concat(
            [real_routes,
             drt.run_matrix_request(locations.iloc[np.r_[ir1[0]:ir1[1], ir2[0]:ir2[1]]]),],
            axis=0)
    #
    # for part in range(math.ceil(locations.shape[0] / osrm_tab_req_limit)):
    #     print(part * osrm_tab_req_limit, min(locations.shape[0], (part + 1) * osrm_tab_req_limit) - 1)
    #     real_routes = pd.concat(
    #         [real_routes,
    #          drt.run_matrix_request(locations.iloc[part * osrm_tab_req_limit:min(locations.shape[0],
    #                                                          (part + 1) * osrm_tab_req_limit)])],
    #         axis=0)

    deadruntimes = pd.concat([deadruntimes, real_routes.reset_index(drop=True)], axis=1)

    deadruntimes = deadruntimes.rename(columns={
        'ID_x': 'FromStopID',
        'ID_y': 'ToStopID',
        'distances': 'Distance',
        'durations': 'RunTime',
    })
    deadruntimes['FromTime'] = 0
    deadruntimes['ToTime'] = 3600 * 32

    deadruntimes[['FromStopID', 'ToStopID']] = deadruntimes[['FromStopID', 'ToStopID']].replace(stoppoints_translation)
    deadruntimes = deadruntimes[
        ['FromStopID', 'ToStopID', 'FromTime', 'ToTime', 'Distance', 'RunTime']].drop_duplicates().dropna()
    deadruntimes.to_csv(os.path.join(config['out_directory'], 'deadruntime.txt'), index=False, sep=';')

    ### $CONNECTIONS
    ### $CONNECTIONS:FromStopID;ToStopID;FromLineID;ToLineID;MinTransferTime
    ### https://developers.google.com/transit/gtfs/reference/#transferstxt
    connections = input_tables['transfers.txt'].copy()
    connections = connections[
        (connections['from_stop_id'].isin(stoppoints['ID'])) & (connections['to_stop_id'].isin(stoppoints['ID'])) & (
                    connections['transfer_type'] == 1)]

    connections = connections.rename(columns={
        'from_stop_id': 'FromStopID',
        'to_stop_id': 'ToStopID',
        'from_route_id': 'FromLineID',
        'to_route_id': 'ToLineID',
        'min_transfer_time': 'MinTransferTime',
    })
    connections[['FromStopID', 'ToStopID', 'FromLineID', 'ToLineID', 'MinTransferTime']].to_csv(
        os.path.join(config['out_directory'], 'connections.txt'), index=False, sep=';')

    return stoppoints, servicejourney, line, connections, deadruntimes


def main():
    mp.freeze_support()
    config = load_config()

    stoppoints, servicejourney, line, connections, deadruntimes = do_the_magic(config)

    mod.merge_output_data(config)


if __name__ == '__main__':
    main()
