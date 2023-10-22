"""
Utilities for reading from and writing to a database using SQLAlchemy.

This module provides a set of tools encapsulated within a class and helper functions 
that allow seamless reading from and writing to a PostgreSQL database. The primary class,
`_db_api`, acts as a connection manager and executor of SQL commands, while the helper 
functions, `read` and `write`, provide more user-friendly interfaces for database interactions.
"""

import os
import logging

from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
from tabulate import tabulate
from pathlib import Path

PROJECT_DIR_PATH = Path(__file__).resolve().parents[1]

load_dotenv(PROJECT_DIR_PATH / ".env")

Base = declarative_base()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

POSTGRES_DB_SERVER = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
engine = create_engine(POSTGRES_DB_SERVER, connect_args={'options': '-csearch_path=relational'})

logger = logging.getLogger(__name__)

load_dotenv(PROJECT_DIR_PATH / ".env")


class _db_api:
    """
    A class to interact with a PostgreSQL database using SQLAlchemy.
    """

    def __init__(self):
        """
        Initialize the _db_api class and connect to the database.
        """
        self.engine = self.connect_to_db()
        self.set_search_path()

    def connect_to_db(self):
        """
        Connect to the PostgreSQL database using information from environment variables.

        Returns:
            Engine object: SQLAlchemy engine.
        """
        db_url = POSTGRES_DB_SERVER
        return create_engine(db_url)

    def set_search_path(self):
        """
        Set the search path for the current connection to use the 'public' schema.
        """
        with self.engine.begin() as conn:
            conn.execute(text("SET search_path TO public;"))

    def _read(self, query, params=None):
        """
        Execute a SQL read query and return the result.

        Parameters:
            query (str): The SQL query to execute.
            params (dict, optional): Parameters for the SQL query.

        Returns:
            Tuple: Result data and columns names.
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query).bindparams(**params if params else {}))
                data = result.fetchall()
                columns = result.keys()
        except SQLAlchemyError as e:
            logger.error(f"Query caused this error: {e}")
            data, columns = None, None
        return data, columns
    
    def _write(self, query, params=None):
        """
        Execute a SQL write query.

        Parameters:
            query (str): The SQL query to execute.
            params (dict, optional): Parameters for the SQL query.
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(text(query).bindparams(**params if params else {}))
            logger.info("Data written to database.")
        except SQLAlchemyError as e:
            logger.error(f"Failed to write to database due to this error: {e}")

_api = _db_api()

def read(query, **kwargs):
    """
    Read data from the database and return it as a DataFrame.

    Parameters:
        query (str): The SQL query to execute.
        **kwargs:
            params (dict, optional): Parameters for the SQL query.
            verbose (bool, optional): If True, print the result. Default is True.

    Returns:
        DataFrame: The result of the query.
    """
    data, columns = _api._read(query, params=kwargs.get('params'))
    if data is None or columns is None:
        logger.error("Query returned None.")
        return None
    df = pd.DataFrame(data, columns=columns)
    if kwargs.get('verbose', True):
        print(tabulate(df, headers='keys', tablefmt='rounded_outline'))
    return df

def write(query, **kwargs):
    """
    Write data to the database.

    Parameters:
        query (str): The SQL query to execute.
        **kwargs:
            params (dict, optional): Parameters for the SQL query.
    """
    _api._write(query, params=kwargs.get('params'))
