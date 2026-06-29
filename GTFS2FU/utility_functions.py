from haversine import Unit, haversine
import pandas as pd
from shapely import MultiPoint
import math

from GTFS2FU import routing as drt

osrm_tab_req_limit = 100
def is_plausibly_germany(lat, lon):
    return 47.0 <= lat <= 55.5 and 5.5 <= lon <= 15.5

def shape2distance(locations, **args):
    locations.reset_index(inplace=True)

    locations = pd.concat([locations,locations.shift(-1)[['shape_pt_lat', 'shape_pt_lon']].rename(columns={
        'shape_pt_lat': 'lat_shifted',
        'shape_pt_lon': 'lon_shifted',
    })], axis=1)

    locations.dropna(inplace=True)

    distances = locations.apply(
        lambda x: haversine(
            x[['shape_pt_lat', 'shape_pt_lon']],
            x[['lat_shifted', 'lon_shifted']],
            unit=Unit.METERS,
        ), axis=1)
    if 'index' in args:
        return (args['index'], round(distances.sum(),0))
    return round(distances.sum(),0)

def stopsequence2distance(locations, **args):
    locations.set_index('stop_sequence', inplace=True)
    locations = pd.concat([locations, locations.shift(1)[['stop_lat', 'stop_lon']].rename(columns={
        'stop_lat': 'lat_shifted',
        'stop_lon': 'lon_shifted',
    })], axis=1)

    locations.dropna(subset=['stop_lat', 'stop_lon', 'lat_shifted', 'lon_shifted'], inplace=True)

    distances = locations.apply(
        lambda x: haversine(
            (x['stop_lat'], x['stop_lon']),
            (x['lat_shifted'], x['lon_shifted']),
            unit=Unit.METERS,
        ), axis=1)
    if 'index' in args:
        return (args['index'], round(distances.sum(),0))
    return round(distances.sum(),0)


def to_edges(trips, **args):

    return pd.Series({
        'service_id': trips['service_id'].iloc[0],
        # 'trip_id': trips['trip_id'].iloc[0],
        'trip_id': args['index'],
        'route_id': trips['route_id'].iloc[0],
        'from': trips['stop_id'].iloc[0],
        'dep': trips['departure_time'].iloc[0],
        'to': trips['stop_id'].iloc[-1],
        'arr': trips['arrival_time'].iloc[-1],
        'vehicle_type': trips['route_type'].iloc[0],
        'distance': trips['distance'].iloc[0]
    })


def to_edge(x, **args):
    global osrm_tab_req_limit
    path_dist = 0
    locations = pd.DataFrame(
        x.loc[:, ['stop_lon', 'stop_lat']].round(6).astype(str).apply(lambda a: ','.join(a), axis=1), columns=['start'])
    locations.loc[:, 'ID'] = x['ID']
    locations.drop_duplicates(subset='ID', keep='last', inplace=True)

    distances = pd.DataFrame()

    ### OSRM limits table requests to 100 locations
    for part in range(math.ceil(locations.shape[0] / osrm_tab_req_limit)):
        distances = pd.concat([distances, drt.run_matrix_request(
            locations.iloc[part * osrm_tab_req_limit:min(locations.shape[0], (part + 1) * osrm_tab_req_limit)])],
                              axis=0)

    for pt in range(locations.shape[0] - 1):
        tmp_distance = distances.loc[(distances['start'] == locations.iloc[pt]['ID']) &
                                     (distances['dest'] == locations.iloc[pt + 1]['ID']), 'distances'].iloc[0]

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
        'distance': round(path_dist, 0)
    })


def find_center_coordinates(a):
    if a.shape[0] < 2: return a[['stop_lat', 'stop_lon']]
    poly = MultiPoint(a[['stop_lat', 'stop_lon']].to_dict(orient='split')['data'])
    cent = poly.centroid
    return pd.DataFrame({'stop_lat': [cent.x], 'stop_lon': [cent.y]})
