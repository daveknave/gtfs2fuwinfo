import requests as req
import pandas as pd
import json, yaml
import time
from io import StringIO

def run_request(point_from, point_to):
    fields = {
        # 'continue_straight': 'true',
        'annotations' : 'distance'
    }
    here_req = req.get(f'http://router.project-osrm.org/route/v1/driving/{point_from};{point_to}', fields)


    if here_req.status_code == 200:
        return json.loads(here_req.content)['routes'][0]['distance']
    else:
        print(here_req.url)
        print(here_req.content)

    time.sleep(1)
    return None

def run_matrix_request(locations, **args):
    fields = {
        'annotations': 'duration,distance'
    }
    if 'get_params' in args:
        fields.update(args)

    start_list_string = ';'.join(locations['start'].tolist())

    osrm_req = req.get(f'http://router.project-osrm.org/table/v1/driving/{start_list_string}', fields)

    if osrm_req.status_code == 200:
        # print(osrm_req.url)
        json_data = json.loads(osrm_req.content)
        json_data.pop('code')
        response_data_df = pd.DataFrame(json_data)

        durations_df = response_data_df['durations']
        durations_df = durations_df.apply(lambda a: pd.Series(a))
        durations_df = durations_df.unstack().reset_index().rename(
            columns={0: 'durations', 'level_0': 'start', 'level_1': 'dest'})

        distances_df = response_data_df['distances']
        distances_df = distances_df.apply(lambda a: pd.Series(a))
        distances_df = distances_df.unstack().reset_index().rename(
            columns={0: 'distances', 'level_0': 'start', 'level_1': 'dest'})

        complete = durations_df.merge(
            distances_df,
            how='inner', on=['start', 'dest'])

        complete[['start', 'dest']] = complete[['start', 'dest']].replace(locations['ID'].reset_index(drop=True).to_dict())

        # complete.to_csv('distances.csv', index=False)

        return complete
    else:
        print(osrm_req.url)
        print(osrm_req.content)

    time.sleep(1)
    return None

if __name__ == '__main__':
    print('Function testing')

    with open('../config.yaml', 'r') as fh:
        config = yaml.load(fh, Loader=yaml.FullLoader)

