import requests as req
import pandas as pd
import json, yaml
import time
from typing import Optional, Any
from here_location_services import LS
from here_location_services.config.matrix_routing_config import WorldRegion
from datetime import datetime
from loguru import logger


def run_request(
    point_from: str, point_to: str, dep_time: str, key: str
) -> Optional[dict[str, Any]]:
    fields = {
        "origin": point_from,
        "destination": point_to,
        "transportMode": "bus",
        "apikey": key,
        "return": "summary,travelSummary",
        "departureTime": dep_time,
    }

    here_req = req.get("https://router.hereapi.com/v8/routes", params=fields)

    if here_req.status_code == 200:
        try:
            return json.loads(here_req.content)["routes"][0]["sections"][0]["summary"]

        except (KeyError, IndexError) as e:
            print(f"Error accessing summary {e}")
            print(here_req.content)
    else:
        print(here_req.url)
        print(here_req.content)

    time.sleep(1)
    return None


def run_matrix_request(od_matrix, dep_time, key):
    url = f"https://matrix.router.hereapi.com/v8/matrix?apiKey={key}&async=false"

    headers = {"Content-Type": "application/json"}
    print(od_matrix.head())
    data = {
        # "regionDefinition": {"type": "circle"}, ##########
        "transportMode": "bus",
        "matrixAttributes": ["travelTimes", "distances"],
        "regionDefinition": {
            "type": "circle",
            "center": {"lat": 52.497225, "lng": 13.395195},
            "radius": 17900,
        },
        # "return": ["summary"],
        "departureTime": dep_time,
        "origins": [
            {"lat": float(start.split(",")[0]), "lng": float(start.split(",")[1])}
            for start in od_matrix["start"]
        ],
        "destinations": [
            {"lat": float(dest.split(",")[0]), "lng": float(dest.split(",")[1])}
            for dest in od_matrix["destination"]
        ],
    }

    here_req = req.post(url, headers=headers, json=data)
    if here_req.status_code == 200:
        response_data = here_req.json()
        print(response_data)
        return pd.DataFrame(response_data["matrix"]["travelTimes"])
    else:
        print(here_req.url)
        print(here_req.content)

    time.sleep(1)
    return None


if __name__ == "__main__":
    print("Function testing")

    with open("config.yaml", "r") as fh:
        config = yaml.load(fh, Loader=yaml.FullLoader)
    result = run_request(
        "52.5308,13.3847", "52.5323,13.3789", "2023-03-24T10:30:00", config["here_key"]
    )
    print(result)
