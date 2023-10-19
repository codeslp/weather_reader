from collections import Counter
from http import HTTPStatus
import httpx
import logging
import logging_config
from pandas import DataFrame
import pandas as pd
from download_common import DownloadStatus, API_KEY
from download_seq import download_one
from datetime import datetime
import asyncio


async def get_weather_async(
    client: httpx.AsyncClient, base_url: str, lat: float, lon: float
) -> (str, dict):
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
    items = {}
    for k, v in nested_dict.items():
        new_key = f"{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(await flatten_nested_dict_async(v, new_key, sep=sep))
        else:
            items[new_key] = v

    return items


async def flatten_reading_json_async(city: str, reading: dict) -> DataFrame:
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
    df, counter = asyncio.run(supervisor(city_lat_lon, base_url, max_concur_req))

    return (df, counter)
