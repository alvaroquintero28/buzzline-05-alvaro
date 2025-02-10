#####################################
# Import Modules
#####################################

# import from standard library
import os
import pathlib
import sqlite3
import datetime

# import from third party libraries
import pandas as pd

# import from local modules
import utils.utils_config as config  # Assuming this is your config module
from utils.utils_logger import logger  # Assuming this is your logger module


#####################################
# Define Function to Initialize SQLite Database
#####################################


def init_db(db_path: pathlib.Path):
    """Initializes the SQLite database; creates the table if it doesn't exist."""
    logger.info(f"Calling SQLite init_db() with {db_path=}.")
    try:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            logger.info("SUCCESS: Got a cursor to execute SQL.")
            cursor.execute("DROP TABLE IF EXISTS streamed_messages;")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS streamed_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message TEXT,
                    author TEXT,
                    timestamp TEXT,
                    category TEXT,
                    sentiment REAL,
                    keyword_mentioned TEXT,
                    message_length INTEGER
                )
            """
            )
            conn.commit()
        logger.info(f"SUCCESS: Database initialized and table ready at {db_path}.")
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize a sqlite database at {db_path}: {e}")


#####################################
# Define Function to Insert a Processed Message into the Database
#####################################


def insert_message(message: dict, db_path: pathlib.Path) -> None:
    """Inserts a single processed message into the SQLite database."""
    logger.info("Calling SQLite insert_message() with:")
    logger.info(f"{message=}")
    logger.info(f"{db_path=}")

    STR_PATH = str(db_path)
    try:
        with sqlite3.connect(STR_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO streamed_messages (
                    message, author, timestamp, category, sentiment, keyword_mentioned, message_length
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    message["message"],
                    message["author"],
                    message["timestamp"],
                    message["category"],
                    message["sentiment"],
                    message["keyword_mentioned"],
                    message["message_length"],
                ),
            )
            conn.commit()
        logger.info("Inserted one message into the database.")
    except Exception as e:
        logger.error(f"ERROR: Failed to insert message into the database: {e}")


#####################################
# Define Function to Delete a Message from the Database
#####################################


def delete_message(message_id: int, db_path: pathlib.Path) -> None:
    """Deletes a message from the SQLite database by its ID."""
    STR_PATH = str(db_path)
    try:
        with sqlite3.connect(STR_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM streamed_messages WHERE id = ?", (message_id,))
            conn.commit()
        logger.info(f"Deleted message with id {message_id} from the database.")
    except Exception as e:
        logger.error(f"ERROR: Failed to delete message from the database: {e}")


#####################################
# Define Functions to Retrieve and Analyze Data
#####################################

def get_all_messages(db_path: pathlib.Path) -> pd.DataFrame:
    """Retrieves all messages from the database."""
    try:
        conn = sqlite3.connect(str(db_path))
        df = pd.read_sql_query("SELECT * FROM streamed_messages", conn)
        conn.close()
        return df
    except Exception as e:
        logger.error(f"Error retrieving messages: {e}")
        return pd.DataFrame()


def get_keywords_by_frequency(db_path: pathlib.Path, top_n: int = 10) -> pd.DataFrame:
    """Retrieves keywords and their frequencies."""
    try:
        conn = sqlite3.connect(str(db_path))
        df = pd.read_sql_query("SELECT keyword_mentioned, COUNT(*) AS frequency FROM streamed_messages GROUP BY keyword_mentioned ORDER BY frequency DESC LIMIT ?", conn, params=(top_n,))
        conn.close()
        return df
    except Exception as e:
        logger.error(f"Error retrieving keyword frequencies: {e}")
        return pd.DataFrame()


def get_messages_by_category(db_path: pathlib.Path, category: str) -> pd.DataFrame:
    """Retrieves messages for a specific category."""
    try:
        conn = sqlite3.connect(str(db_path))
        df = pd.read_sql_query("SELECT * FROM streamed_messages WHERE category = ?", conn, params=(category,))
        conn.close()
        return df
    except Exception as e:
        logger.error(f"Error retrieving messages by category: {e}")
        return pd.DataFrame()

def get_messages_in_time_range(db_path: pathlib.Path, start_time: str, end_time: str) -> pd.DataFrame:
    """Retrieves messages within a specific time range."""
    try:
        conn = sqlite3.connect(str(db_path))
        df = pd.read_sql_query("SELECT * FROM streamed_messages WHERE timestamp BETWEEN ? AND ?", conn, params=(start_time, end_time))
        conn.close()
        return df
    except Exception as e:
        logger.error(f"Error retrieving messages by time range: {e}")
        return pd.DataFrame()


def analyze_keyword_trends(df: pd.DataFrame) -> dict:
    """Analyzes keyword trends from a DataFrame."""
    keyword_counts = df.groupby('keyword_mentioned')['keyword_mentioned'].count().sort_values(ascending=False)
    return keyword_counts.to_dict()


def analyze_category_trends(df: pd.DataFrame) -> dict:
    """Analyzes category trends from a DataFrame."""
    category_counts = df.groupby('category')['category'].count().sort_values(ascending=False)
    return category_counts.to_dict()

def analyze_author_trends(df:pd.DataFrame) -> dict:
    """Analyzes author trends from a DataFrame."""
    author_counts = df.groupby('author')['author'].count().sort_values(ascending=False)
    return author_counts.to_dict()


#####################################
# Define main() function for testing
#####################################
def main():
    logger.info("Starting db testing.")

    # Use config to make a path to a parallel test database
    DATA_PATH: pathlib.Path = config.get_base_data_path
    TEST_DB_PATH: pathlib.Path = DATA_PATH / "test_buzz.sqlite"

    # Initialize the SQLite database by passing in the path
    init_db(TEST_DB_PATH)
    logger.info(f"Initialized database file at {TEST_DB_PATH}.")

    test_message = {
        "message": "I just shared a meme! It was amazing.",
        "author": "Charlie",
        "timestamp": "2025-01-29 14:35:20",
        "category": "humor",
        "sentiment": 0.87,
        "keyword_mentioned": "meme",
        "message_length": 42,
    }

    insert_message(test_message, TEST_DB_PATH)

    # Retrieve the ID of the inserted test message
    try:
        with sqlite3.connect(TEST_DB_PATH, timeout=1.0) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id FROM streamed_messages WHERE message = ? AND author = ?",
                (test_message["message"], test_message["author"]),
            )
            row = cursor.fetchone()
            if row:
                test_message_id = row[0]
                # Delete the test message
                delete_message(test_message_id, TEST_DB_PATH)
            else:
                logger.warning("Test message not found; nothing to delete.")
    except Exception as e:
        logger.error(f"ERROR: Failed to retrieve or delete test message: {e}")

    #Analyze data
    df = get_all_messages(TEST_DB_PATH)
    keyword_trends = analyze_keyword_trends(df)
    category_trends = analyze_category_trends(df)
    author_trends = analyze_author_trends(df)

    logger.info(f"Keyword Trends: {keyword_trends}")
    logger.info(f"Category Trends: {category_trends}")
    logger.info(f"Author Trends: {author_trends}")

    logger.info("Finished testing.")


# #####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
