"""
This module provides utilities for local development and testing.
"""

from pathlib import Path
from os import environ

import configparser
import toml


def get_env_var_config() -> dict:
    """
    Returns a dictionary of the connection parameters using the SnowSQL CLI
    environment variables.
    """
    try:
        return {
            "user": environ["SNOWSQL_USER"],
            "password": environ["SNOWSQL_PWD"],
            "account": environ["SNOWSQL_ACCOUNT"],
            # "role": environ["SNOWSQL_ROLE"],
            "warehouse": environ["SNOWSQL_WAREHOUSE"],
            "database": environ["SNOWSQL_DATABASE"],
            "schema": environ["SNOWSQL_SCHEMA"],
        }
    except KeyError as exc:
        raise KeyError(
            "ERROR: Environment variable for Snowflake Connection not found. "
            + "Please set the SNOWSQL_* environment variables"
        ) from exc


def get_dev_config(
    config_name: str = "preprod",
    app_config_path: Path = Path.cwd().joinpath("app.toml"),
) -> dict:
    """
    Returns a section of a toml file as a dictionary.
    """
    try:
        app_config = toml.load(app_config_path)
        return app_config[config_name]
    except Exception as exc:
        raise EnvironmentError(
            "Error creating snowpark session - be sure you've logged into "
            "the SnowCLI and have a valid app.toml file",
        ) from exc
