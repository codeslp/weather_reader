import httpx
import logging
import logging.config
from collections import Counter
from pandas import DataFrame
import pandas as pd
from download_common import DownloadStatus
from download_seq import download_one


def download_many(
    base_url: str, city_lat_lon: dict, concur_type: str, max_concur_req: int
) -> (DataFrame, Counter):
    """
    Download weather data for multiple cities concurrently using thread or process pools.

    Args:
        base_url (str): The base URL for the weather API.
        city_lat_lon (dict): A dictionary containing city information with latitude and longitude coordinates.
        concur_type (str): The concurrency type, either "thread" or "process".
        max_concur_req (int): The maximum number of concurrent requests to be made.

    Returns:
        tuple: A tuple containing a DataFrame with weather data and a Counter object
        tracking the download status.

    Raises:
        Exception: If an error occurs during the download process, an error message is logged.

    Example:
        df, counter = download_many(base_url, city_lat_lon, "thread", 5)
    """
    counter: Counter[DownloadStatus] = Counter()
    dataframes = []
    if concur_type == "thread":
        from concurrent.futures import ThreadPoolExecutor as PoolExecutor, as_completed
    elif concur_type == "process":
        from concurrent.futures import ProcessPoolExecutor as PoolExecutor, as_completed
    with PoolExecutor(max_workers=max_concur_req) as executor:
        to_do_map = {}
        for city, data in city_lat_lon.items():
            city_entry = {city: data}
            future = executor.submit(download_one, base_url, city_entry)
            to_do_map[future] = city
        done_iter = as_completed(to_do_map)
        for future in done_iter:
            try:
                df, status = future.result()
                dataframes.append(df)
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
