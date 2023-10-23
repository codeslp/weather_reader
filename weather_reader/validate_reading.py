from pydantic import BaseModel, Field, ValidationError
from typing import Optional
from datetime import datetime
import logging
import pandas as pd


logger = logging.getLogger(__name__)


class Reading(BaseModel):
    lat: float = Field(..., gt=-90, lt=90)
    lon: float = Field(..., gt=-180, lt=180)
    timezone: str
    timezone_offset: int
    dt: int
    sunrise: int
    sunset: int
    temp: float
    feels_like: float
    pressure: float
    humidity: float = Field(..., ge=0, le=100)
    dew_point: float
    uvi: float
    clouds: float
    visibility: float
    wind_speed: float
    wind_deg: float
    wind_gust: float
    id: int
    main: str
    description: str
    icon: str
    rain_1h: Optional[float] = 0
    snow_1h: Optional[float] = 0
    timestamp: datetime
    city: str


def validate_reading(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate rows in a DataFrame using a Pydantic model.

    This function takes a DataFrame as input and validates each row using a Pydantic model called `Reading`.
    Rows that fail validation are logged as errors, and valid rows are counted and logged as information.

    Args:
        df (pd.DataFrame): The DataFrame containing the rows to be validated.

    Returns:
        pd.DataFrame: The same DataFrame passed as input.

    Example:
        validated_df = validate_reading(my_dataframe)
    """
    invalid_rows = []
    valid_rows_count = 0

    for index, row in df.iterrows():
        try:
            reading = Reading(**row.to_dict())
            valid_rows_count += 1

        except ValidationError as e:
            invalid_rows.append((index, str(e)))
            logger.error(f"Validation Error at row {index}: {str(e)}")

    logger.info(f"Total Valid Rows: {valid_rows_count}")

    if invalid_rows:
        logger.warning(f"Total Invalid Rows: {len(invalid_rows)}")
    else:
        logger.info("All rows are valid.")

    return df
