import pandas as pd
import os
from pargroupby import pargroupby
from scipy.spatial.distance import hamming

data_dir = './data/GTFS'
# os.chdir('./gtfs2fuwinfo')

input_tables = {}

for f in os.listdir(data_dir):
    tmp_df = pd.read_csv(data_dir + '/' + f, delimiter=',', decimal='.', quotechar='"')
    input_tables[f] = tmp_df

tr_df = input_tables['trips.txt'].merge(input_tables['routes.txt'], on='route_id')
tr_df = tr_df[(tr_df['agency_id'] == 796) & (tr_df['route_type'] == 700)]
# %%
ts_df = tr_df.merge(input_tables['stop_times.txt'], on='trip_id', how='left')
str_df = ts_df.merge(input_tables['stops.txt'], on='stop_id')
# %%

def to_edge(x, g):
    x = x.sort_values('stop_sequence')

    path_dist = 0
    for pt in range(x.shape[1] - 1):
        path_dist += hamming(x.loc[:, ['stop_lat', 'stop_lon']].iloc[pt], x.loc[:, ['stop_lat', 'stop_lon']].iloc[pt+1])

    return pd.Series(data={
        'trip_id'       : x['trip_id'].iloc[0],
        'route_id'      : x['route_id'].iloc[0],
        'from'          : x['stop_id'].iloc[0],
        'dep'           : x['departure_time'].iloc[0],
        'to'            : x['stop_id'].iloc[-1],
        'arr'           : x['arrival_time'].iloc[-1],
        'min_dwell'     : 0,
        'vehicle_type'  : x['vehicle_type'].iloc[0],
        'backshift'     : 0,
        'forwardshift'  : 0
    })
sjdf = pargroupby(gr=str_df.head(1000).groupby('trip_id'), func=to_edge, name='2edges', ncores=3)
# %%
print(sjdf)
# %%
servicejourneys = ts_df.groupby('trip_id')
# %%
### $STOPPOINTS

# %%
print('test')


