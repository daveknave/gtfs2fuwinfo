import requests as req
import pandas as pd
import json, yaml
import time

def run_request(point_from, point_to, dep_time, key):
    fields = {
        'origin': point_from,
        'destination': point_to,
        'transportMode': 'bus',
        'apikey': key,
        'summaryAttributes': 'traveltime ,distance',
        'departureTime': dep_time
    }

    here_req = req.get('https://router.hereapi.com/v8/routes', fields)


    if here_req.status_code == 200:
        return json.loads(here_req.content)['routes'][0]['sections'][0]['summary']
    else:
        print(here_req.url)
        print(here_req.content)

    time.sleep(1)
    return None

def run_matrix_request(od_matrix, dep_time, key):
    fields = {
        'transportMode': 'bus',
        'apikey': key,
        'return': 'summary',
        'departureTime': dep_time
    }

    fields.update(dict(zip(['start'+str(i) for i in range(od_matrix.shape[0])], od_matrix['start'].tolist())))
    fields.update(dict(zip(['destination'+str(i) for i in range(od_matrix.shape[0])], od_matrix['destination'].tolist())))

    here_req = req.get('https://matrix.route.ls.hereapi.com/routing/7.2/calculatematrix.json', fields)


    if here_req.status_code == 200:
        return pd.DataFrame(json.loads(here_req.content)['response']['matrixEntry']['summary'])
    else:
        print(here_req.url)
        print(here_req.content)

    time.sleep(1)
    return None

if __name__ == '__main__':
    print('Function testing')

    with open('config.yaml', 'r') as fh:
        config = yaml.load(fh, Loader=yaml.FullLoader)
    result = run_request('52.5308,13.3847', '52.5323,13.3789', '2023-03-24T10:30:00', config['here_key'])
    print(result)
