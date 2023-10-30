import logging
from logging_config import configure_logger
import time
from datetime import datetime
import json
from urllib.parse import urlencode
import httpx
from pathlib import Path

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

configure_logger()

PROJECT_DIR_PATH = Path(__file__).resolve().parents[1]
CITIES_CONFIG_PATH = PROJECT_DIR_PATH / "weather_reader" / "cities.json"


def update_city_lat_lon() -> dict:
    """
    Update or retrieve latitude and longitude coordinates for a list of cities from a JSON file.

    This function checks if the latitude and longitude coordinates of the cities in the JSON file are up to date.
    If not, it updates the coordinates and writes them back to the file.

    Returns:
        dict: A dictionary containing city information with updated 'lat' and 'lon' coordinates.
    """
    try:
        with open(CITIES_CONFIG_PATH, "r") as file:
            data = json.load(file)
            cities = data.get("cities", {})
            city_lat_lon = data.get("city_lat_lon", {})
            cached_city_lat_lon = data.get("cached_city_lat_lon", {})

        cities_key_set = set(cities.keys())
        if (cities_key_set != set(city_lat_lon.keys())) and cities_key_set.issubset(
            set(cached_city_lat_lon.keys())
        ):
            logging.info(
                "Attempting to update city latitude and longitude coordinates from cache."
            )
            logging.info(f"Starting update at: {datetime.now()}")
            keys_to_update = cities_key_set - set(city_lat_lon.keys())
            for key in keys_to_update:
                city_lat_lon[key] = cached_city_lat_lon[key]
            data["city_lat_lon"] = city_lat_lon
            with open(CITIES_CONFIG_PATH, "w") as file:
                json.dump(data, file, indent=4)
            logging.info(f"Finished update at: {datetime.now()}")

        elif cities_key_set != set(city_lat_lon.keys()):
            logging.info("Updating city latitude and longitude coordinates.")
            logging.info(f"Starting update at: {datetime.now()}")
            updated_city_lat_lon = get_lat_lon(cities)
            data["city_lat_lon"] = updated_city_lat_lon
            with open(CITIES_CONFIG_PATH, "w") as file:
                json.dump(data, file, indent=4)
            logging.info(f"Finished update at: {datetime.now()}")



        return city_lat_lon

    except FileNotFoundError:
        logging.error(f"Could not find the file at {CITIES_CONFIG_PATH}")
    except KeyError as e:
        logging.error(f"Unexpected JSON format: Missing key {e}")


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

    city_lat_lon = update_city_lat_lon()
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
    print(main(concur_type='coroutine'))
