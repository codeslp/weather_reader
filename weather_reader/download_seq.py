from collections import Counter
from datetime import datetime
import logging
import logging_config
from urllib.parse import urlencode

import httpx
from http import HTTPStatus
import pandas as pd
from pandas import DataFrame

from download_common import DownloadStatus, save_to_pq, API_KEY


def get_weather(base_url: str, lat: float, lon: float) -> (str, dict):
    """
    Get weather data for a specific latitude and longitude.

    Args:
        base_url (str): The base URL of the weather API.
        lat (float): The latitude coordinate.
        lon (float): The longitude coordinate.

    Returns:
        tuple: A tuple containing a string and a dictionary.
            - The string represents the response status.
            - The dictionary contains weather data.

    Raises:
        httpx.HTTPError: If an HTTP error occurs during the request.
    """
    url = f"{base_url}lat={lat}&lon={lon}&exclude=hourly,daily,minutely,alerts&appid={API_KEY}"
    resp = httpx.get(url)
    resp.raise_for_status()
    return resp.json()


def download_one(base_url: str, city_lat_long_row: dict) -> (DataFrame, DownloadStatus):
    """
    Download weather data for a city's latitude and longitude.

    Args:
        base_url (str): The base URL of the weather API.
        city_lat_long_row (dict): A dictionary containing city information.

    Returns:
        tuple: A tuple containing a DataFrame and a DownloadStatus.
            - The DataFrame contains weather data.
            - The DownloadStatus indicates the success or failure of the download.

    Raises:
        httpx.HTTPError: If an HTTP error occurs during the request.
    """
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
    """
    Flatten a nested dictionary into a flat dictionary.

    Args:
        nested_dict (dict): The nested dictionary to be flattened.
        parent_key (str, optional): The parent key used for prefixing keys.
        sep (str, optional): The separator used to separate keys in the flattened dictionary.

    Returns:
        dict: A flat dictionary containing flattened key-value pairs from the nested dictionary.
    """
    items = {}
    for k, v in nested_dict.items():
        new_key = f"{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_nested_dict(v, new_key, sep=sep))
        else:
            items[new_key] = v

    return items


def flatten_reading_json(city: str, reading: dict) -> DataFrame:
    """
    Flatten a JSON response from weather data into a DataFrame and add city
    and timestamp columns.

    Args:
        city (str): The name of the city for which the weather data is retrieved.
        reading (dict): The nested JSON response containing weather data.

    Returns:
        pandas.DataFrame: A DataFrame containing flattened weather data.
    """
    flattened_data = flatten_nested_dict(reading)

    weather_info = reading["current"]["weather"][0]
    flattened_data.update(flatten_nested_dict(weather_info, "weather"))

    flattened_data["wind_gust"] = reading["current"].get("wind_gust", 0)
    flattened_data["rain_1h"] = reading["current"].get("rain", {}).get("1h", 0)
    flattened_data["snow_1h"] = reading["current"].get("snow", {}).get("1h", 0)

    df = pd.DataFrame([flattened_data])
    df["timestamp"] = datetime.now()
    df["city"] = city

    df.drop(columns=["weather", "1h"], errors="ignore", inplace=True)

    return df


def download_many(base_url: str, city_lat_lon: dict) -> (DataFrame, Counter):
    """
    Download weather data for multiple cities and aggregate the results.

    Args:
        base_url (str): The base URL for weather data API.
        city_lat_lon (dict): A dictionary containing city information including latitude and longitude.

    Returns:
        tuple: A tuple containing a DataFrame with weather data and a Counter with download status counts.
    """
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
