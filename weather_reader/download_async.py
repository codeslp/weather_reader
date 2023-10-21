from collections import Counter
from datetime import datetime
import asyncio
import httpx
import logging
from pandas import DataFrame
import pandas as pd
from download_common import DownloadStatus, API_KEY
from http import HTTPStatus


async def get_weather_async(
    client: httpx.AsyncClient, base_url: str, lat: float, lon: float
) -> (str, dict):
    """
    Asynchronously fetches weather data for a specific latitude and longitude.

    Args:
        client (httpx.AsyncClient): The asynchronous HTTP client.
        base_url (str): The base URL for the weather API.
        lat (float): The latitude coordinate of the location.
        lon (float): The longitude coordinate of the location.

    Returns:
        tuple: A tuple containing the city name (str) and a dictionary of weather data.

    Raises:
        httpx.HTTPError: If an HTTP error occurs while fetching the data.

    Example:
        city, weather_data = await get_weather_async(client, base_url, lat, lon)
    """
    url = f"{base_url}lat={lat}&lon={lon}&exclude=hourly,daily,minutely,alerts&appid={API_KEY}"
    resp = await client.get(url)
    resp.raise_for_status()
    return resp.json()


async def download_one_async(
    client: httpx.AsyncClient,
    base_url: str,
    semaphore: asyncio.Semaphore,
    city_lat_long_row: dict,
) -> (DataFrame, DownloadStatus):
    """
    Asynchronously downloads weather data for a list of cities.

    Args:
        client (httpx.AsyncClient): The asynchronous HTTP client.
        base_url (str): The base URL for the weather API.
        semaphore (asyncio.Semaphore): A semaphore for limiting concurrent downloads.
        city_lat_long_row (dict): A dictionary containing city names and latitude-longitude coordinates.

    Returns:
        tuple: A tuple containing a DataFrame with downloaded data and a DownloadStatus.

    Example:
        df, status = await download_one_async(client, base_url, semaphore, city_lat_long_row)
    """
    status = DownloadStatus.ERROR
    try:
        async with semaphore:
            for city, data in city_lat_long_row.items():
                lat = data.get("lat")
                lon = data.get("lon")
                logging.info(f"Getting weather for: {city} {lat}, {lon}")
                reading = await get_weather_async(client, base_url, lat, lon)
    except httpx.HTTPError as e:
        res = e.response
        if res.status_code == HTTPStatus.NOT_FOUND:
            status = DownloadStatus.NOT_FOUND
            logging.error(f"{city} {lat}, {lon} not found: {res.url}")
        else:
            logging.error(f"HTTP Error for {city} {lat}, {lon}: {e}")
    else:
        df = await flatten_reading_json_async(city, reading)
        status = DownloadStatus.OK
        logging.info(f"Successful retrieval for: {city} {lat}, {lon}")

    return (df, status)


async def flatten_nested_dict_async(nested_dict, parent_key="", sep="_") -> dict:
    """
    Recursively flattens a nested dictionary into a flat dictionary.

    Args:
        nested_dict (dict): The nested dictionary to be flattened.
        parent_key (str): The parent key for nested dictionaries (used recursively).
        sep (str, optional): The separator used to join keys when flattening.

    Returns:
        dict: A flat dictionary containing flattened key-value pairs.

    Example:
        flattened_data = await flatten_nested_dict_async(nested_dict)
    """
    items = {}
    for k, v in nested_dict.items():
        new_key = f"{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(await flatten_nested_dict_async(v, new_key, sep=sep))
        else:
            items[new_key] = v

    return items


async def flatten_reading_json_async(city: str, reading: dict) -> DataFrame:
    """
    Flattens a JSON weather reading into a pandas DataFrame.

    Args:
        city (str): The name of the city for the weather reading.
        reading (dict): The nested JSON weather reading.

    Returns:
        pandas.DataFrame: A DataFrame containing the flattened weather data.

    Example:
        df = await flatten_reading_json_async(city, reading)
    """

    flattened_data = await flatten_nested_dict_async(reading)

    weather_info = reading["current"]["weather"][0]
    weather_info_flat = await flatten_nested_dict_async(weather_info, "weather")
    flattened_data.update(weather_info_flat)

    flattened_data["wind_gust"] = reading["current"].get("wind_gust", 0)
    flattened_data["rain_1h"] = reading["current"].get("rain", {}).get("1h", 0)
    flattened_data["snow_1h"] = reading["current"].get("snow", {}).get("1h", 0)

    df = pd.DataFrame([flattened_data])
    df["timestamp"] = datetime.now()
    df["city"] = city

    df.drop(columns=["weather"], errors="ignore", inplace=True)

    return df


async def supervisor(
    city_lat_lon: dict, base_url: str, concur_req: int = None
) -> (DataFrame, Counter):
    """
    Coordinate and manage asynchronous weather data downloads for multiple cities.

    This function manages the asynchronous download of weather data for multiple cities,
    coordinating the download tasks and handling errors. It uses asynchronous features
    to optimize concurrent requests.

    Args:
        city_lat_lon (dict): A dictionary containing city information with 'lat' and 'lon' coordinates.
        base_url (str): The base URL for the weather data API.
        concur_req (int, optional): The maximum number of concurrent requests. If not provided, downloads are sequential.

    Returns:
        Tuple[pandas.DataFrame, collections.Counter]: A tuple containing a DataFrame of weather data
        for all cities and a Counter object tracking the download status.

    Example:
        df, counter = await supervisor(city_lat_lon, base_url, concur_req=5)
    """
    counter: Counter[DownloadStatus] = Counter()
    semaphore = None
    if concur_req:
        semaphore = asyncio.Semaphore(concur_req)
    dataframes = []
    async with httpx.AsyncClient() as client:
        to_do = [
            download_one_async(client, base_url, semaphore, {city: data})
            for city, data in city_lat_lon.items()
        ]
        to_do_iter = asyncio.as_completed(to_do)
        error: httpx.HTTPError | None = None
        for coro in to_do_iter:
            try:
                one_response = await coro
                df = one_response[0]
                if df is not None:
                    dataframes.append(df)
                status = one_response[1]
            except httpx.HTTPStatusError as exc:
                error_msg = "HTTP error {resp.status_code} - {resp.reason_phrase}"
                error_msg = error_msg.format(resp=exc.response)
                error = exc
            except httpx.RequestError as exc:
                error_msg = f"{exc} {type(exc)}".strip()
                error = exc
            except KeyboardInterrupt:
                break

            if error:
                status = DownloadStatus.ERROR
                logging.error(error_msg)
            counter[status] += 1

        df = pd.concat(dataframes, ignore_index=True)

    return (df, counter)


def download_many(
    base_url: str, city_lat_lon: dict, max_concur_req: int
) -> (DataFrame, Counter):
    """
    Download weather data for multiple cities concurrently.

    This function downloads weather data for multiple cities concurrently using the specified number
    of concurrent requests. It coordinates the download tasks and returns a DataFrame containing
    weather data for all cities along with a Counter object tracking the download status.

    Args:
        base_url (str): The base URL for the weather data API.
        city_lat_lon (dict): A dictionary containing city information with 'lat' and 'lon' coordinates.
        max_concur_req (int): The maximum number of concurrent requests to use for downloads.

    Returns:
        Tuple[pandas.DataFrame, collections.Counter]: A tuple containing a DataFrame of weather data
        for all cities and a Counter object tracking the download status.

    Example:
        df, counter = download_many(base_url, city_lat_lon, max_concur_req=5)
    """
    df, counter = asyncio.run(supervisor(city_lat_lon, base_url, max_concur_req))

    return (df, counter)
