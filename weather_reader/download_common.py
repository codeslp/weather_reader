import logging
import os
import time
from datetime import datetime
from enum import Enum
from pathlib import Path
from collections import Counter

import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv


PROJECT_DIR_PATH = Path(__file__).resolve().parents[1]
DATA_DIR_PATH = PROJECT_DIR_PATH / "data_lake"
DATA_DIR_PATH.mkdir(parents=True, exist_ok=True)

load_dotenv(PROJECT_DIR_PATH / ".env")
API_KEY = os.getenv("API_KEY")

SERVERS = {
    "LATLONG": "http://api.openweathermap.org/geo/1.0/direct?",
    "WEATHER": "http://api.openweathermap.org/data/3.0/onecall?",
}

DownloadStatus = Enum("DownloadStatus", "OK NOT_FOUND ERROR")



def initial_report(actual_args, city_lat_lon) -> None:
    """
    Log the initial report for weather data retrieval.

    Args:
        actual_args (tuple): A tuple containing concurrency type and max concurrency.
        city_lat_lon (dict): A dictionary containing city information with latitude and longitude.

    Returns:
        None
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Getting weather for: {list(city_lat_lon.keys())}")
    logger.info(f"Concurrency type: {actual_args[0]}")
    logger.info(f"Max concurrency: {actual_args[1]}")
    logger.info(f"Time started: {datetime.now()}")


def save_to_pq(df) -> None:
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
    """
    Log the final report for weather data retrieval.

    Args:
        counter (collections.Counter): A Counter object with download status counts.
        start_time (datetime.datetime): The start time of the data retrieval process.

    Returns:
        None
    """
    elapsed = time.perf_counter() - start_time
    plural = "s" if counter[DownloadStatus.OK] != 1 else ""
    logging.info(f"{counter[DownloadStatus.OK]:3} reading{plural} downloaded.")
    if counter[DownloadStatus.NOT_FOUND]:
        logging.error(f"{counter[DownloadStatus.NOT_FOUND]:3} not found.")
    if counter[DownloadStatus.ERROR]:
        plural = "s" if counter[DownloadStatus.ERROR] != 1 else ""
        logging.error(f"{counter[DownloadStatus.ERROR]:3} error{plural}.")
    logging.info(f"Elapsed time: {elapsed:.2f}s")
