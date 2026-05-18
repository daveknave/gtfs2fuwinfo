#! /usr/bin/python3.11
import statistics

import numpy as np
import pandas as pd
import os, importlib, datetime

from shapely.ops import orient

import pargroupby
importlib.reload(pargroupby)

from shapely.geometry import MultiPoint
import retrieve_deadruntime as drt
importlib.reload(drt)
import multiprocessing as mp
import yaml

def to_edge(x, **args):
    path_dist = 0
    locations = pd.DataFrame(x.loc[:, ['stop_lon', 'stop_lat']].round(6).astype(str).apply(lambda a:','.join(a), axis=1), columns=['start'])
    locations.loc[:,'ID'] = x['ID']
    locations.drop_duplicates(subset='ID', keep='last', inplace=True)

    distances = drt.run_matrix_request(locations)
    for pt in range(locations.shape[0] - 2):
        tmp_distance = distances.loc[(distances['start'] == locations.iloc[pt]['ID']) &
            (distances['dest'] == locations.iloc[pt+1]['ID']), 'distances'].iloc[0]

        path_dist += tmp_distance

    return pd.Series({
        'service_id': x['service_id'].iloc[0],
        'trip_id': x['trip_id'].iloc[0],
        'route_id': x['route_id'].iloc[0],
        'from': x['stop_id'].iloc[0],
        'dep': x['departure_time'].iloc[0],
        'to': x['stop_id'].iloc[-1],
        'arr': x['arrival_time'].iloc[-1],
        'vehicle_type': x['route_type'].iloc[0],
        'distance': round(path_dist,0)
    })
# %%
def find_center_coordinates(a):
    if a.shape[0] < 2: return a[['stop_lat', 'stop_lon']]
    poly = MultiPoint(a[['stop_lat', 'stop_lon']].to_dict(orient='split')['data'])
    cent = poly.centroid
    return pd.DataFrame({'stop_lat': [cent.x], 'stop_lon': [cent.y]})


def do_the_magic(config):
    global to_edge
    ### Read GTFS data files
    data_dir = config['in_directory']

    input_tables = {}
    for f in os.listdir(data_dir):
        if not '.txt' in f: continue
        tmp_df = pd.read_csv(os.path.join(data_dir, f), delimiter=',', decimal='.', quotechar='"')
        input_tables[f] = tmp_df

    ### Prepare data
    tr_df = input_tables['trips.txt'].merge(input_tables['routes.txt'], on='route_id')
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
    print(d1.shape, d2.shape)

    tr_df = tr_df.set_index('trip_id')
    tr_df['valid'] = False
    tr_df.loc[d1.index,'valid'] = True
    tr_df.loc[d2[d2['exception_type'] == 1].index, 'valid'] = True
    tr_df.loc[d2[d2['exception_type'] == 2].index, 'valid'] = False

    ### Select bus services
    ts_df = tr_df[tr_df['valid']].merge(input_tables['stop_times.txt'], on='trip_id', how='left')

    stp_df = input_tables['stops.txt']
    stp_df.loc[:,'ID'] = stp_df.apply(lambda a: a['stop_id'] if ':' in a['stop_id'] else a['parent_station'], axis=1)
    stp_df.loc[np.logical_not(stp_df['ID'].isna()),'ID'] = stp_df.loc[np.logical_not(stp_df['ID'].isna()),'ID'].apply(lambda a: a.split(':')[2])
    stp_df.loc[stp_df['ID'].isna(),'ID'] = stp_df.loc[stp_df['ID'].isna(),'stop_id']

    tmp_stp_df = stp_df.set_index('ID')
    # tmp_stp_df.loc[:,['stop_lat', 'stop_lon']] = pargroupby.do(gr=stp_df.groupby('ID'), func=find_center_coordinates, name='coords', ncores=8)
    tmp_stp_df.loc[:,['stop_lat', 'stop_lon']] = stp_df.groupby('ID').apply(lambda a: find_center_coordinates(a)).reset_index().set_index('ID')[['stop_lat', 'stop_lon']]
    stp_df = tmp_stp_df.reset_index()

    ### Create stoppoint ID translation table
    tmp_dict = stp_df[['stop_id', 'ID']].to_dict(orient='list')
    stoppoints_translation = dict(zip(tmp_dict['stop_id'], tmp_dict['ID']))

    str_df = ts_df.merge(stp_df, how="left", on='stop_id').drop_duplicates().sort_values(['trip_id', 'stop_sequence'])

    sjdf = pargroupby.do(gr=str_df[str_df.columns].groupby('trip_id'), func=to_edge, name='2edges', ncores=8)

    ### Generate Output-Data

    ### $STOPPOINTS
    ### $STOPPOINT:ID;Code;Name;VehCapacityForCharging
    stoppoints = str_df[['ID', 'stop_code','stop_name','stop_lat','stop_lon']].drop_duplicates()

    stoppoints = stoppoints.rename(columns={
        'stop_id'        : 'ID',
        'stop_code'      : 'Code',
        'stop_name'      : 'Name',
        'stop_lat'      : 'Lat',
        'stop_lon'      : 'Lon',
    })
    stoppoints['VehCapacityForCharging'] = 0

    stoppoints.to_csv(os.path.join(config['out_directory'],'stoppoints.txt'), index=False, sep=';')


    ### $SERVICEJOURNEY
    ### $SERVICEJOURNEY:ID;LineID;FromStopID;ToStopID;DepTime;ArrTime;MinAheadTime;MinLayoverTime;VehTypeGroupID;MaxShiftBackwardSeconds;MaxShiftForwardSeconds;Distance
    sjdf['min_dwell'] = 0
    sjdf['min_ahead'] = 0
    sjdf['backshift'] = 0
    sjdf['forwardshift'] = 0

    servicejourney = sjdf.rename(columns={
        'trip_id'       : 'ID',
        'route_id'      : 'LineID',
        'from'          : 'FromStopID',
        'to'            : 'ToStopID',
        'dep'           : 'DepTime',
        'arr'           : 'ArrTime',
        'min_ahead'     : 'MinAheadTime',
        'min_dwell'     : 'MinLayoverTime',
        'vehicle_type'  : 'VehTypeGroupID',
        'backshift'     : 'MaxShiftBackwardSeconds',
        'forwardshift'  : 'MaxShiftForwardSeconds',
        'distance'      : 'Distance',
    })
    servicejourney.drop(columns=['service_id'], inplace=True)
    servicejourney[['FromStopID', 'ToStopID']] = servicejourney[['FromStopID', 'ToStopID']].replace(stoppoints_translation)

    servicejourney.to_csv(os.path.join(config['out_directory'],'servicejourney.txt'), index=False, sep=';')

    # %%
    ### $LINE
    ### $LINE:ID;Code;Name
    line = str_df[['route_id', 'route_short_name']].drop_duplicates()
    line = line.rename(columns={
        'route_id'              : 'ID',
        'route_short_name'      : 'Code',
    })
    line['Name'] = line['Code']
    line.to_csv(os.path.join(config['out_directory'],'line.txt'), index=False, sep=';')

    # %%
    ### $DEADRUNTIME
    ### $DEADRUNTIME:FromStopID;ToStopID;FromTime;ToTime;Distance;RunTime
    sp_red = stoppoints[(stoppoints['ID'].isin(servicejourney['FromStopID'])) | (stoppoints['ID'].isin(servicejourney['ToStopID'])) | (stoppoints['Code'] == 'DEPOT')]

    # Create Deadhead matrix
    sp_red['key'] = 1
    deadruntimes = sp_red.merge(sp_red, on="key")

    locations = pd.DataFrame(columns=['start', 'ID'])
    locations['start'] = (deadruntimes.loc[deadruntimes['ID_x'] != deadruntimes['ID_y'], :].apply(lambda x: ','.join([str(x['Lon_x']),str(x['Lat_x'])]), axis=1)).unique()
    locations['ID'] = deadruntimes['ID_x']

    real_routes = drt.run_matrix_request(locations)
    deadruntimes = pd.concat([deadruntimes,real_routes], axis=1)

    deadruntimes = deadruntimes.rename(columns={
        'ID_x'          : 'FromStopID',
        'ID_y'          : 'ToStopID',
        'distances'     : 'Distance',
        'durations'     : 'RunTime',
    })
    deadruntimes['FromTime'] = 0
    deadruntimes['ToTime'] = 3600*32

    deadruntimes[['FromStopID', 'ToStopID']] = deadruntimes[['FromStopID', 'ToStopID']].replace(stoppoints_translation)
    deadruntimes = deadruntimes[['FromStopID','ToStopID','FromTime','ToTime','Distance','RunTime']].drop_duplicates().dropna()
    deadruntimes.to_csv(os.path.join(config['out_directory'],'deadruntime.txt'), index=False, sep=';')

    ### $CONNECTIONS
    ### $CONNECTIONS:FromStopID;ToStopID;FromLineID;ToLineID;MinTransferTime
    ### https://developers.google.com/transit/gtfs/reference/#transferstxt
    connections = input_tables['transfers.txt'].copy()
    connections = connections[ (connections['from_stop_id'].isin(stoppoints['ID'])) & (connections['to_stop_id'].isin(stoppoints['ID'])) & (connections['transfer_type'] == 1)]

    connections = connections.rename(columns={
        'from_stop_id'          : 'FromStopID',
        'to_stop_id'            : 'ToStopID',
        'from_route_id'         : 'FromLineID',
        'to_route_id'           : 'ToLineID',
        'min_transfer_time'     : 'MinTransferTime',
    })
    connections[['FromStopID','ToStopID','FromLineID','ToLineID','MinTransferTime']].to_csv(os.path.join(config['out_directory'],'connections.txt'), index=False, sep=';')

    return stoppoints, servicejourney, line, connections, deadruntimes

if __name__ == '__main__':
    mp.freeze_support()
    with open('config.yaml', 'r') as fh:
        config = yaml.load(fh, Loader=yaml.FullLoader)

    stoppoints, servicejourney, line, connections, deadruntimes = do_the_magic(config)
