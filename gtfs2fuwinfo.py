#! /usr/bin/python3
import pandas as pd
import os, importlib
import pargroupby
import dask
from scipy.spatial.distance import hamming, euclidean
import haversine

data_dir = './data/GTFS'

input_tables = {}

for f in os.listdir(data_dir):
    tmp_df = pd.read_csv(data_dir + '/' + f, delimiter=',', decimal='.', quotechar='"')
    input_tables[f] = tmp_df

tr_df = input_tables['trips.txt'].merge(input_tables['routes.txt'], on='route_id')
tr_df = tr_df[(tr_df['agency_id'] == 796) & (tr_df['route_type'] == 700)]

ts_df = tr_df.merge(input_tables['stop_times.txt'], on='trip_id', how='left')
ts_df['stop_id'] = '0' + ts_df['stop_id'].apply(str)
str_df = ts_df.merge(input_tables['stops.txt'], on='stop_id').drop_duplicates()
del ts_df
del tr_df
# %%
importlib.reload(pargroupby)
def to_edge(x, g):
    x = x.sort_values('stop_sequence')

    path_dist = 0
    for pt in range(x.shape[0] - 1):
        path_dist += euclidean(list(x.loc[:, ['stop_lat', 'stop_lon']].iloc[pt]), list(x.loc[:, ['stop_lat', 'stop_lon']].iloc[pt+1]))

    return {
        'trip_id'       : x['trip_id'].iloc[0],
        'route_id'      : x['route_id'].iloc[0],
        'from'          : x['stop_id'].iloc[0],
        'dep'           : x['departure_time'].iloc[0],
        'to'            : x['stop_id'].iloc[-1],
        'arr'           : x['arrival_time'].iloc[-1],
        'vehicle_type'  : x['route_type'].iloc[0],
        'distance'      : path_dist
    }

sjdf = pargroupby.do(gr=str_df.groupby('trip_id'), func=to_edge, name='2edges', ncores=4)
# %%
sjdf.to_csv('servicejourney.csv', index=False)
# %%
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

servicejourney.to_csv('servicejourney.txt')
# %%
### $STOPPOINTS
### $STOPPOINT:ID;Code;Name;VehCapacityForCharging
stoppoints = str_df[['stop_id', 'stop_code', 'stop_name', 'stop_lat', 'stop_lon']].drop_duplicates()

stoppoints = stoppoints.rename(columns={
    'stop_id'        : 'ID',
    'stop_code'      : 'Code',
    'stop_name'      : 'Name',
    'stop_lat'      : 'Lat',
    'stop_lon'      : 'Lon',
})
stoppoints['VehCapacityForCharging'] = 1
stoppoints.to_csv('stoppoints.txt')
# %%
### $LINE
### $LINE:ID;Code;Name
line = str_df[['route_id', 'route_short_name']].drop_duplicates()
line = line.rename(columns={
    'route_id'              : 'ID',
    'route_short_name'      : 'Code',
})
line['Name'] = line['Code']
line.to_csv('line.txt')
# %%
sp_red = stoppoints[(stoppoints['ID'].isin(sjdf['from'])) | (stoppoints['ID'].isin(sjdf['to']))]
print(sp_red.shape)
# %%
### $DEADRUNTIME
### $DEADRUNTIME:FromStopID;ToStopID;FromTime;ToTime;Distance;RunTime
# Create Deadheadmatrix
sp_red['key'] = 1
crossprod = sp_red.merge(sp_red, on="key")
crossprod['distance'] = crossprod[crossprod['ID_x'] != crossprod['ID_y']].apply(lambda x: haversine.haversine([x['Lat_x'],x['Lon_x']],[x['Lat_y'],x['Lon_y']], unit=haversine.Unit.METERS), axis=1)

crossprod = crossprod.rename(columns={
    'ID_x'          : 'FromStopID',
    'ID_y'          : 'ToStopID',
    'distance'      : 'Distance',
})
crossprod['FromTime'] = 0
crossprod['ToTime'] = 0
crossprod['RunTime'] = 60 * crossprod['Distance'] / 25
crossprod = crossprod[['FromStopID','ToStopID','FromTime','ToTime','Distance','RunTime']].drop_duplicates().dropna()[crossprod['Distance'] < 3000]
crossprod.to_csv('deadruntime.txt')
# %%
