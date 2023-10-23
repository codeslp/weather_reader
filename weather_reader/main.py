# import os

# os.chdir(os.path.dirname(os.path.abspath(__file__)))

import logging
import time
from datetime import datetime
from urllib.parse import urlencode
from enum import Enum
import httpx
import shelve

from download_common import (
    save_to_pq,
    save_to_db,
    initial_report,
    final_report,
    SERVERS,
    API_KEY,
)
from download_async import download_many as download_readings_async
from download_concur import download_many as download_readings_concur
from download_seq import download_many as download_readings_seq
from validate_reading import validate_reading


cities = {
    "Istanbul": {"country": "TR"},
    "London": {"country": "GB"},
    "Saint Petersburg": {"country": "RU"},
    "Berlin": {"country": "DE"},
    "Madrid": {"country": "ES"},
    "Kyiv": {"country": "UA"},
    "Rome": {"country": "IT"},
    "Bucharest": {"country": "RO"},
    "Paris": {"country": "FR"},
    "Minsk": {"country": "BY"},
    "Vienna": {"country": "AT"},
    "Warsaw": {"country": "PL"},
    "Hamburg": {"country": "DE"},
    "Budapest": {"country": "HU"},
    "Belgrade": {"country": "RS"},
    "Barcelona": {"country": "ES"},
    "Munich": {"country": "DE"},
    "Kharkiv": {"country": "UA"},
    "Milan": {"country": "IT"},
    "Prague": {"country": "CZ"},
}


def update_city_lat_lon(cities: dict) -> dict:
    """
    Update or retrieve latitude and longitude coordinates for a list of cities.

    This function either updates the latitude and longitude coordinates of the provided cities
    or retrieves them if not already available in the `cities` dictionary. If the coordinates
    are retrieved or updated, they are stored in a dictionary with city names as keys and
    dictionaries containing 'lat' and 'lon' coordinates as values.

    Args:
        cities (dict): A dictionary containing city information, including country and state (if applicable).

    Returns:
        dict: A dictionary containing city information with 'lat' and 'lon' coordinates.
    """

    with shelve.open("city_lat_lon") as db:
        if db:
            city_lat_lon = db["city_lat_lon"]
        else:
            city_lat_lon = None

    if (city_lat_lon is None) or (cities.keys() != city_lat_lon.keys()):
        logging.info("Updating city latitude and longitude coordinates.")
        logging.info("Starting update at: " + str(datetime.now()))
        city_lat_lon = get_lat_lon(cities)
        logging.info("Finished update at: " + str(datetime.now()))
        with shelve.open("city_lat_lon") as db:
            db["city_lat_lon"] = city_lat_lon

    return city_lat_lon


def get_lat_lon(cities: dict) -> dict:
    """
    Retrieves latitude and longitude coordinates for a list of cities.

    Args:
        cities (dict): A dictionary containing city information, including country and state (if applicable).

    Returns:
        dict: A dictionary containing city information with added 'lat' and 'lon' coordinates.
    """

    base_url = SERVERS["LATLONG"]

    for city, co_st in cities.items():
        country_code = co_st["country"]

        if country_code == "US":
            q_value = f"{city},{co_st['state']},{country_code}"
        else:
            q_value = f"{city},{country_code}"

        params = {"q": q_value, "limit": 1, "appid": API_KEY}

        url = base_url + urlencode(params)
        response = httpx.get(url)
        response.raise_for_status()
        data = response.json()
        cities[city]["lat"] = data[0]["lat"]
        cities[city]["lon"] = data[0]["lon"]

    return cities


def main(concur_type=None, max_concur_req=None):
    """
    Main function to orchestrate the data download and processing workflow.

    This function retrieves latitude and longitude coordinates for a list of cities,
    downloads weather data based on the specified concurrency type, and saves the
    valid data to a Parquet file. It also generates and logs initial and final reports
    including concurrency type, max concurrency, and elapsed time.

    Args:
        concur_type (str, optional): The concurrency type to use for downloading data
            (e.g., 'thread', 'process', 'coroutine'). Default is None.
        max_concur_req (int, optional): The maximum number of concurrent requests to
            make during data download. Default is None.

    Returns:
        None
    """

    city_lat_lon = update_city_lat_lon(cities)
    initial_report((concur_type, max_concur_req), city_lat_lon)
    t0 = time.perf_counter()

    if concur_type == "thread":
        result = download_readings_concur(
            SERVERS["WEATHER"], city_lat_lon, "thread", max_concur_req
        )
    elif concur_type == "process":
        if __name__ == "__main__":
            result = download_readings_concur(
                SERVERS["WEATHER"], city_lat_lon, "process", max_concur_req
            )
    elif concur_type == "coroutine":
        if __name__ == "__main__":
            result = download_readings_async(
                SERVERS["WEATHER"], city_lat_lon, max_concur_req=len(city_lat_lon)
            )
    else:
        if __name__ == "__main__":
            result = download_readings_seq(SERVERS["WEATHER"], city_lat_lon)

    df = result[0]
    counter = result[1]
    valid_readings_batch = validate_reading(df)
    save_to_pq(valid_readings_batch)
    save_to_db(valid_readings_batch)

    final_report(counter, t0)

    return df


if __name__ == "__main__":
    print(main(concur_type="coroutine"))
