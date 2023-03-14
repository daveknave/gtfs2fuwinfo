import requests as req
import pandas as pd
import json


def run_request(point_from, point_to, time, key):
    fields = {
        'origin': point_from,
        'destination': point_to,
        'transportMode': 'bus',
        'apikey': key,
        'return': 'summary',
        'departureTime': time
    }

    here_req = req.get('https://router.hereapi.com/v8/routes', fields)


    if here_req.status_code == 200:
        return json.loads(here_req.content)['routes'][0]['sections'][0]['summary']
    else:
        print(here_req.content)

    return None


if __name__ == '__main__':
    print('Function testing')
    result = run_request('52.5308,13.3847', '52.5323,13.3789', '2023-03-24T10:30:00')
