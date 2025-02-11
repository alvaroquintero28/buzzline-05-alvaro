"""
Config Utility
File: utils/utils_config.py

This script provides the configuration functions for the project. 

It centralizes the configuration management 
by loading environment variables from .env in the root project folder
 and constructing file paths using pathlib. 

If you rename any variables in .env, remember to:
- recopy .env to .env.example (and hide the secrets)
- update the corresponding function in this module.
"""

#####################################
# Imports
#####################################

import os
import pathlib
from typing import Union
from dotenv import load_dotenv
from .utils_logger import logger


#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Helper Function for Loading Config Values
#####################################

def _get_config_value(key: str, default: Union[str, int], type_: type, redact: bool = False) -> Union[str, int]:
    """Helper to fetch and type-check config values from environment variables."""
    value = os.getenv(key, str(default))
    try:
        value = type_(value)
    except ValueError:
        logger.error(f"Error converting environment variable '{key}' to type {type_.__name__}. Using default.")
        return default
    if redact and key == "POSTGRES_PASSWORD":
        logger.info(f"{key}: [REDACTED]")
        return "[REDACTED]"
    logger.info(f"{key}: {value}")
    return value

#####################################
# Getter Functions for .env Variables
#####################################

def get_zookeeper_address() -> str:
    return _get_config_value("ZOOKEEPER_ADDRESS", "127.0.0.1:2181", str)


def get_kafka_broker_address() -> str:
    return _get_config_value("KAFKA_BROKER_ADDRESS", "127.0.0.1:9092", str)


def get_kafka_topic() -> str:
    return _get_config_value("BUZZ_TOPIC", "buzzline", str)


def get_message_interval_seconds() -> int:
    return _get_config_value("MESSAGE_INTERVAL_SECONDS", 5, int)


def get_kafka_consumer_group_id() -> str:
    return _get_config_value("BUZZ_CONSUMER_GROUP_ID", "buzz_group", str)


def get_base_data_path() -> pathlib.Path:
    project_root = pathlib.Path(__file__).parent.parent
    data_dir_str = _get_config_value("BASE_DATA_DIR", "data", str)
    data_dir = project_root / data_dir_str
    return data_dir.resolve() # Resolve to the absolute path


def get_live_data_path() -> pathlib.Path:
    live_data_path = get_base_data_path() / _get_config_value("LIVE_DATA_FILE_NAME", "project_live.json", str)
    return live_data_path.resolve()


def get_sqlite_path() -> pathlib.Path:
    sqlite_path = get_base_data_path() / _get_config_value("SQLITE_DB_FILE_NAME", "buzz.sqlite", str)
    return sqlite_path.resolve()


def get_database_type() -> str:
    return _get_config_value("DATABASE_TYPE", "sqlite", str)


def get_postgres_host() -> str:
    return _get_config_value("POSTGRES_HOST", "localhost", str)


def get_postgres_port() -> int:
    return _get_config_value("POSTGRES_PORT", 5432, int)


def get_postgres_db() -> str:
    return _get_config_value("POSTGRES_DB", "postgres_buzz_database", str)


def get_postgres_user() -> str:
    return _get_config_value("POSTGRES_USER", "your_username", str)


def get_postgres_password() -> str:
    return _get_config_value("POSTGRES_PASSWORD", "your_password", str, redact=True)


def get_mongodb_uri() -> str:
    return _get_config_value("MONGODB_URI", "mongodb://localhost:27017/", str)


def get_mongodb_db() -> str:
    return _get_config_value("MONGODB_DB", "mongo_buzz_database", str)


def get_mongodb_collection() -> str:
    return _get_config_value("MONGODB_COLLECTION", "mongo_buzz_collection", str)


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    # Test the configuration functions
    logger.info("Testing configuration.")
    try:
        get_zookeeper_address()
        get_kafka_broker_address()
        get_kafka_topic()
        get_message_interval_seconds()
        get_kafka_consumer_group_id()
        get_base_data_path()
        get_live_data_path()
        get_sqlite_path()
        get_database_type()
        get_postgres_host()
        get_postgres_port()
        get_postgres_db()
        get_postgres_user()
        get_postgres_password()
        get_mongodb_uri()
        get_mongodb_db()
        get_mongodb_collection()
        logger.info("SUCCESS: Configuration function tests complete.")
    except Exception as e:
        logger.exception(f"ERROR: Configuration function test failed: {e}")


