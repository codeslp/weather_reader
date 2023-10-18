from collections import Counter
from http import HTTPStatus

import httpx
import logging
import logging_config
from urllib.parse import urlencode
import pandas as pd
from pandas import DataFrame
from datetime import datetime

from download_common import save_to_pq, API_KEY, city_lat_long, DownloadStatus
from validate_reading import validate_reading

base_url = "https://api.openweathermap.org/data/3.0/onecall?"


def get_weather(base_url: str, lat: float, lon: float) -> dict:
    url = f"{base_url}lat={lat}&lon={lon}&exclude=hourly,daily,minutely,alerts&appid={API_KEY}"
    resp = httpx.get(url)
    resp.raise_for_status()
    return resp.json()


def download_one(city_lat_long: dict) -> DownloadStatus:
    try:
        for city, data in city_lat_long.items():
            print(city, data)
            lat = data.get("lat")
            lon = data.get("lon")
        logging.info(f"Getting weather for: {city} {lat}, {lon}")
        reading = get_weather(base_url, lat, lon)

    except httpx.HTTPError as e:
        res = e.response
        if res.status_code == HTTPStatus.NOT_FOUND:
            status = DownloadStatus.NOT_FOUND
            logging.error(f"{city} {lat}, {lon} not found: {res.url}")
        else:
            raise

    else:
        df = flatten_reading_json(reading)
        status = DownloadStatus.OK
        logging.info(f"Successful retrieval for: {city} {lat}, {lon}")
        validate_reading(df)
        # save_to_pq(df)

    return status


def flatten_nested_dict(nested_dict, parent_key="", sep="_"):
    items = {}
    for k, v in nested_dict.items():
        new_key = f"{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_nested_dict(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items


def flatten_reading_json(reading: dict) -> pd.DataFrame:
    flattened_data = flatten_nested_dict(reading)

    weather_info = reading["current"]["weather"][0]
    flattened_data.update(flatten_nested_dict(weather_info, "weather"))

    flattened_data["wind_gust"] = reading["current"].get("wind_gust", 0)
    flattened_data["rain_1h"] = reading["current"].get("rain", {}).get("1h", 0)
    flattened_data["snow_1h"] = reading["current"].get("snow", {}).get("1h", 0)

    df = pd.DataFrame([flattened_data])
    df["timestamp"] = datetime.now()
    df["city"] = list(city_lat_long.keys())[0]

    df.drop(columns=["weather"], errors="ignore", inplace=True)

    return df


city_lat_long = {"Istanbul": {"country": "TR", "lat": 41.0091982, "lon": 28.9662187}}
download_one(city_lat_long)
