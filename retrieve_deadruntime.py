import requests as req
import pandas as pd
import json, yaml
import time
from typing import Optional, Any
from here_location_services import LS
from here_location_services.config.matrix_routing_config import WorldRegion
from datetime import datetime
from loguru import logger
import os


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
            logger.error(f"Error accessing summary {e}")
            logger.error(here_req.content)
    else:
        logger.error(here_req.url)
        logger.error(here_req.content)

    time.sleep(1)
    return None


def run_matrix_request(
    od_matrix: pd.DataFrame, dep_time: datetime, key: str
) -> pd.DataFrame:

    url = f"https://matrix.router.hereapi.com/v8/matrix?apiKey={key}&async=false"

    headers = {"Content-Type": "application/json"}

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
        # print("Response Data:", json.dumps(response_data, indent=4))

        response_data = here_req.json()
        matrix_data = response_data.get("matrix", {})
        travel_times = matrix_data.get("travelTimes", [])
        distances = matrix_data.get("distances", [])

        print(od_matrix["start"])
        print(od_matrix["destination"])

        num_destinations = len(od_matrix["destination"])
        for i in range(len(travel_times)):
            print(od_matrix["destination"].iloc[i % num_destinations])

        result_df = pd.DataFrame(
            {
                "start": [
                    od_matrix["start"].iloc[i // num_destinations]
                    for i in range(len(travel_times))
                ],
                "destination": [
                    od_matrix["destination"].iloc[i % num_destinations]
                    for i in range(len(travel_times))
                ],
                "RunTime": travel_times,
                "Distance": distances,
            }
        )

        # Ausgabe
        print(result_df)

        return result_df

    else:
        if not os.path.exists("error"):
            os.makedirs("error")

        error_filename = f"error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        od_matrix.to_csv(path_or_buf=f"error/{error_filename}")
        logger.error(here_req.url)
        logger.error(here_req.content)

    time.sleep(1)
    return None


if __name__ == "__main__":

    # Testdaten für Berlin (Koordinaten in Lat,Lon-Format)
    od_matrix_data = {
        "start": [
            "52.520008,13.404954",  # Brandenburger Tor
            "52.515503,13.377242",  # Potsdamer Platz
            "52.524722,13.393333",  # Hackescher Markt
        ],
        "destination": [
            "52.517037,13.388860",  # Reichstag
            "52.514444,13.350833",  # Zoologischer Garten
            "52.522222,13.412222",  # Alexanderplatz
        ],
    }

    # DataFrame erstellen
    od_matrix = pd.DataFrame(od_matrix_data)
    result = run_matrix_request(
        od_matrix,
        dep_time="2024-10-24T12:00:00Z",
        key="UyCRFcrH554-j8KIcuXdYa7QxEEmKU9uQUG3NmZk7xA",
    )
    print(result.info())
