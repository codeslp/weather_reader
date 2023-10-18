from dotenv import load_dotenv
import os
from urllib.parse import urlencode
import httpx
from dotenv import load_dotenv
from datetime import datetime
import logging
import logging_config
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from enum import Enum

import time
from collections import Counter

PROJECT_DIR_PATH = Path(__file__).resolve().parents[1]
DATA_DIR_PATH = PROJECT_DIR_PATH / "data"
DATA_DIR_PATH.mkdir(parents=True, exist_ok=True)

load_dotenv(PROJECT_DIR_PATH / ".env")

API_KEY = os.getenv("API_KEY")

SERVERS = {
    'LATLONG': 'http://api.openweathermap.org/geo/1.0/direct?',
    'WEATHER': 'http://api.openweathermap.org/data/3.0/onecall?'
}

DownloadStatus = Enum("DownloadStatus", "OK NOT_FOUND ERROR")

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
}

city_lat_lon = {
    "Istanbul": {"country": "TR", "lat": 41.0091982, "lon": 28.9662187},
    "London": {"country": "GB", "lat": 51.5073219, "lon": -0.1276474},
    "Saint Petersburg": {"country": "RU", "lat": 59.938732, "lon": 30.316229},
    "Berlin": {"country": "DE", "lat": 52.5170365, "lon": 13.3888599},
    "Madrid": {"country": "ES", "lat": 40.4167047, "lon": -3.7035825},
    "Kyiv": {"country": "UA", "lat": 50.4500336, "lon": 30.5241361},
    "Rome": {"country": "IT", "lat": 41.8933203, "lon": 12.4829321},
    "Bucharest": {"country": "RO", "lat": 44.4361414, "lon": 26.1027202},
    "Paris": {"country": "FR", "lat": 48.8588897, "lon": 2.3200410217200766},
    "Minsk": {"country": "BY", "lat": 53.9024716, "lon": 27.5618225},
    "Vienna": {"country": "AT", "lat": 48.2083537, "lon": 16.3725042},
    "Warsaw": {"country": "PL", "lat": 52.2319581, "lon": 21.0067249},
    "Hamburg": {"country": "DE", "lat": 53.550341, "lon": 10.000654},
    "Budapest": {"country": "HU", "lat": 47.4979937, "lon": 19.0403594},
    "Belgrade": {"country": "RS", "lat": 44.8178131, "lon": 20.4568974},
    "Barcelona": {"country": "ES", "lat": 41.3828939, "lon": 2.1774322},
    "Munich": {"country": "DE", "lat": 48.1371079, "lon": 11.5753822},
    "Kharkiv": {"country": "UA", "lat": 49.9923181, "lon": 36.2310146},
    "Milan": {"country": "IT", "lat": 45.4641943, "lon": 9.1896346},
}


def update_city_lat_long(cities=cities, city_lat_lon=None):
    """
    Update or retrieve latitude and longitude coordinates for a list of cities.

    This function either updates the latitude and longitude coordinates of the provided cities
    or retrieves them if not already available in the `city_lat_lon` dictionary. If the `city_lat_lon`
    dictionary does not contain coordinates for all cities, it will be updated.

    Args:
        cities (dict): A dictionary containing city information, including country and state (if applicable).
        city_lat_lon (dict, optional): A dictionary containing city information with 'lat' and 'lon' coordinates.

    Returns:
        dict: A dictionary containing city information with 'lat' and 'lon' coordinates.
    """
    if (city_lat_lon is None) or (cities.keys() != city_lat_lon.keys()):
        logger = logging.getLogger(__name__)
        logging.info("Updating city latitude and longitude coordinates.")
        logging.info("Starting update at: " + str(datetime.now()))
        city_lat_lon = get_lat_long(cities)
        logging.info("Finished update at: " + str(datetime.now()))
    return city_lat_lon


def get_lat_long(cities=cities) -> dict:
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


def initial_report(actual_args, city_lat_lon):
    logger = logging.getLogger(__name__)
    logger.info(f"Getting weather for: {list(city_lat_lon.keys())}")
    logger.info(f"Concurrency type: {actual_args[0]}")
    logger.info(f"Max concurrency: {actual_args[1]}")
    logger.info(f"Time started: {datetime.now()}")


def save_to_pq(df):
    """
    Save a DataFrame to a Parquet file locally.

    This function converts the given DataFrame to a PyArrow Table and writes it to a Parquet file.
    The Parquet file is named with the current timestamp in the format 'YYYY-MM-DD_HH:MM:SS'.

    Args:
        df (pandas.DataFrame): The DataFrame to be saved as a Parquet file.

    Returns:
        None

    Raises:
        Exception: If an error occurs during the Parquet writing process, an error message is logged.

    Example:
        save_to_pq(my_dataframe)
    """
    try:
        readings_dict = pa.Table.from_pandas(df)
        pq_path = (
            DATA_DIR_PATH / f"{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}.parquet"
        )
        pq.write_table(readings_dict, pq_path)
        logging.info(f"Saved to {pq_path}")
    except Exception as e:
        logging.error(f"Error saving to Parquet: {e}")


def final_report(counter: Counter[DownloadStatus], start_time: datetime) -> None:
    elapsed = time.perf_counter() - start_time
    plural = 's' if counter[DownloadStatus.OK] != 1 else ''
    logging.info(f'{counter[DownloadStatus.OK]:3} readings{plural} downloaded.')
    if counter[DownloadStatus.NOT_FOUND]:
        logging.error(f'{counter[DownloadStatus.NOT_FOUND]:3} not found.')
    if counter[DownloadStatus.ERROR]:
        plural = 's' if counter[DownloadStatus.ERROR] != 1 else ''
        logging.error(f'{counter[DownloadStatus.ERROR]:3} error{plural}.')
    logging.info(f'Elapsed time: {elapsed:.2f}s')




