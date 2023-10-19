from collections import Counter
from http import HTTPStatus

import httpx
import logging
import logging_config
from urllib.parse import urlencode
import pandas as pd
from pandas import DataFrame
from datetime import datetime

from download_common import save_to_pq, API_KEY, city_lat_lon, DownloadStatus


def get_weather(base_url: str, lat: float, lon: float) -> (str, dict):
    url = f"{base_url}lat={lat}&lon={lon}&exclude=hourly,daily,minutely,alerts&appid={API_KEY}"
    resp = httpx.get(url)
    resp.raise_for_status()
    return resp.json()


def download_one(base_url: str, city_lat_long_row: dict) -> (DataFrame, DownloadStatus):
    status = DownloadStatus.ERROR
    try:
        for city, data in city_lat_long_row.items():
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
            logging.error(f"HTTP Error for {city} {lat}, {lon}: {e}")

    else:
        df = flatten_reading_json(city, reading)
        status = DownloadStatus.OK
        logging.info(f"Successful retrieval for: {city} {lat}, {lon}")

    return (df, status)


def flatten_nested_dict(nested_dict, parent_key="", sep="_") -> dict:
    items = {}
    for k, v in nested_dict.items():
        new_key = f"{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_nested_dict(v, new_key, sep=sep))
        else:
            items[new_key] = v

    return items


def flatten_reading_json(city: str, reading: dict) -> DataFrame:
    flattened_data = flatten_nested_dict(reading)

    weather_info = reading["current"]["weather"][0]
    flattened_data.update(flatten_nested_dict(weather_info, "weather"))

    flattened_data["wind_gust"] = reading["current"].get("wind_gust", 0)
    flattened_data["rain_1h"] = reading["current"].get("rain", {}).get("1h", 0)
    flattened_data["snow_1h"] = reading["current"].get("snow", {}).get("1h", 0)

    df = pd.DataFrame([flattened_data])
    df["timestamp"] = datetime.now()
    df["city"] = city

    df.drop(columns=["weather"], errors="ignore", inplace=True)

    return df


def download_many(base_url: str, city_lat_lon: dict) -> (DataFrame, Counter):
    counter: Counter[DownloadStatus] = Counter()
    dataframes = []
    for city, data in city_lat_lon.items():
        try:
            one_response = download_one(base_url, {city: data})
            df = one_response[0]
            if df is not None:
                dataframes.append(df)
            status = one_response[1]
        except httpx.HTTPStatusError as exc:
            error_msg = "HTTP error {resp.status_code} - {resp.reason_phrase}"
            error_msg = error_msg.format(resp=exc.response)
        except httpx.RequestError as exc:
            error_msg = f"{exc} {type(exc)}".strip()
        except KeyboardInterrupt:
            break
        else:
            error_msg = ""

        if error_msg:
            status = DownloadStatus.ERROR
            logging.error(error_msg)
        counter[status] += 1

    df = pd.concat(dataframes, ignore_index=True)

    return (df, counter)


# base_url = "https://api.openweathermap.org/data/3.0/onecall?"
# city_lat_lon = {"Istanbul": {"country": "TR", "lat": 41.0091982, "lon": 28.9662187}}
# download_many(base_url, city_lat_lon)
