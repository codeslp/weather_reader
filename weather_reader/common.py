from dotenv import load_dotenv
import os
from urllib.parse import urlencode
import httpx
from dotenv import load_dotenv
from datetime import datetime
import logging
import logging_config


load_dotenv("/Users/bfaris96/Desktop/turing-proj/weather_reader/.env")

API_KEY = os.getenv("API_KEY")

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

city_lat_long = {
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


def update_city_lat_long(cities=cities, city_lat_long=None):
    """
    Update or retrieve latitude and longitude coordinates for a list of cities.

    This function either updates the latitude and longitude coordinates of the provided cities
    or retrieves them if not already available in the `city_lat_long` dictionary. If the `city_lat_long`
    dictionary does not contain coordinates for all cities, it will be updated.

    Args:
        cities (dict): A dictionary containing city information, including country and state (if applicable).
        city_lat_long (dict, optional): A dictionary containing city information with 'lat' and 'lon' coordinates.

    Returns:
        dict: A dictionary containing city information with 'lat' and 'lon' coordinates.
    """
    if (city_lat_long is None) or (cities.keys() != city_lat_long.keys()):
        logger = logging.getLogger(__name__)
        logging.info("Updating city latitude and longitude coordinates.")
        logging.info("Starting update at: " + str(datetime.now()))
        city_lat_long = get_lat_long(cities)
        logging.info("Finished update at: " + str(datetime.now()))
    return city_lat_long


def get_lat_long(cities=cities) -> dict:
    """
    Retrieves latitude and longitude coordinates for a list of cities.

    Args:
        cities (dict): A dictionary containing city information, including country and state (if applicable).

    Returns:
        dict: A dictionary containing city information with added 'lat' and 'lon' coordinates.
    """

    base_url = "http://api.openweathermap.org/geo/1.0/direct?"

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


def initial_report(actual_args, city_lat_long):
    logger = logging.getLogger(__name__)
    logger.info(f"Getting weather for: {list(city_lat_long.keys())}")
    logger.info(f"Concurrency type: {actual_args[0]}")
    logger.info(f"Max concurrency: {actual_args[1]}")
    logger.info(f"Time started: {datetime.now()}")


def main(concur_type, max_concur_req=None):
    # call initial_report

    # logic about what concurrency type to use, make request
    if concur_type == "thread":
        download_readings_concur(city_lat_long, "thread", max_concur_req)
    elif concur_type == "process":
        download_readings_concur(city_lat_long, "process", max_concur_req)
    elif concur_type == "coroutine":
        download_readings_async(city_lat_long, max_concur_req)
    else:
        download_readings(city_lat_long)
    actual_args = (concur_type, max_concur_req)
    # write locally to parquet
    # call valdate operations
    # call transform operations
    # write to db


if __name__ == "__main__":
    print(get_lat_long())
