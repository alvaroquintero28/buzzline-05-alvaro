import logging
import sqlite3
import os
import pathlib
import datetime
import pandas as pd
import json

# PLACEHOLDER - Implement this module (see previous responses for example)
import utils.utils_config as config

# PLACEHOLDER - Implement this module to set up logging
from utils.utils_logger import logger


#####################################
# Define Function to Initialize SQLite Database
#####################################

def init_db(db_path: pathlib.Path):
    logger.info(f"Calling SQLite init_db() with {db_path=}.")
    try:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            logger.info("SUCCESS: Got a cursor to execute SQL.")
            conn.execute("BEGIN TRANSACTION")
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
        logger.exception(f"ERROR: Failed to initialize a sqlite database at {db_path}: {e}")


#####################################
# Define Function to Insert a Processed Message into the Database
#####################################

def insert_message(message: dict, db_path: pathlib.Path) -> None:
    logger.info("Calling SQLite insert_message() with:")
    logger.info(f"{message=}")
    logger.info(f"{db_path=}")

    STR_PATH = str(db_path)
    try:
        with sqlite3.connect(STR_PATH) as conn:
            cursor = conn.cursor()
            logger.info("Database Connection Successful")

            sql = """
                    INSERT INTO streamed_messages (
                        message, author, timestamp, category, sentiment, keyword_mentioned, message_length
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """
            params = (
                message.get("message", ""),
                message.get("author", ""),
                message.get("timestamp", ""),
                message.get("category", ""),
                message.get("sentiment", 0.0),
                message.get("keyword_mentioned", ""),
                message.get("message_length", 0),
            )

            try:
                logger.info(f"SQL command to be executed: {sql}")
                logger.info(f"Parameters: {params}")

                cursor.execute(sql, params)
                conn.commit()
                last_row_id = cursor.lastrowid
                logger.info(f"Inserted one message into the database. Last row ID: {last_row_id}")
                logger.info("Verifying insertion by checking table contents")
                cursor.execute("SELECT * FROM streamed_messages")
                rows = cursor.fetchall()
                for row in rows:
                    logger.info(f"Row: {row}")
            except sqlite3.IntegrityError as e:
                logger.error(f"IntegrityError during insertion: {e}")
            except sqlite3.OperationalError as e:
                logger.error(f"OperationalError during insertion: {e}")
            except Exception as e:
                logger.exception(f"Unexpected error during insertion: {e}")
    except sqlite3.OperationalError as e:
        logger.exception(f"Error connecting to the database: {e}")
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")



#####################################
# Define Function to Delete a Message from the Database
#####################################

def delete_message(message_id: int, db_path: pathlib.Path) -> None:
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
    try:
        conn = sqlite3.connect(str(db_path))
        df = pd.read_sql_query("SELECT * FROM streamed_messages", conn)
        logger.info(f"Data retrieved from database:\n{df}")
        conn.close()
        return df
    except Exception as e:
        logger.error(f"Error retrieving messages: {e}")
        return pd.DataFrame()


def analyze_keyword_trends(df: pd.DataFrame) -> dict:
    keyword_counts = df.groupby('keyword_mentioned')['keyword_mentioned'].count().sort_values(ascending=False)
    return keyword_counts.to_dict()


def analyze_category_trends(df: pd.DataFrame) -> dict:
    category_counts = df.groupby('category')['category'].count().sort_values(ascending=False)
    return category_counts.to_dict()


def analyze_author_trends(df: pd.DataFrame) -> dict:
    author_counts = df.groupby('author')['author'].count().sort_values(ascending=False)
    return author_counts.to_dict()


#####################################
# Define main() function for testing
#####################################

def main():
    logger.info("Starting db testing.")

    DATA_PATH = pathlib.Path(config.get_base_data_path())
    DB_PATH_STRING = config.get_db_path()
    TEST_DB_PATH: pathlib.Path = DATA_PATH / pathlib.Path(DB_PATH_STRING)

    init_db(TEST_DB_PATH)
    logger.info(f"Initialized database file at {TEST_DB_PATH}.")

    # Add multiple test messages
    messages = [
        {"message": "This is a test message.", "author": "User1", "timestamp": datetime.datetime.now().isoformat(), "category": "News", "sentiment": 0.5, "keyword_mentioned": "test", "message_length": 24},
        {"message": "Another test message.", "author": "User2", "timestamp": datetime.datetime.now().isoformat(), "category": "Sports", "sentiment": 0.8, "keyword_mentioned": "sports", "message_length": 21},
        {"message": "Yet another test!", "author": "User1", "timestamp": datetime.datetime.now().isoformat(), "category": "Tech", "sentiment": 0.2, "keyword_mentioned": "AI", "message_length": 17},
    ]

    for msg in messages:
        insert_message(msg, TEST_DB_PATH)

    test_simple_insert(TEST_DB_PATH)

    # Deletion of test message (this section could be improved for more robust testing)
    try:
        with sqlite3.connect(TEST_DB_PATH, timeout=1.0) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM streamed_messages WHERE message = 'Simple Test'")
            row = cursor.fetchone()
            if row:
                test_message_id = row[0]
                delete_message(test_message_id, TEST_DB_PATH)
            else:
                logger.warning("Test message not found; nothing to delete.")
    except Exception as e:
        logger.error(f"ERROR: Failed to retrieve or delete test message: {e}")


    df = get_all_messages(TEST_DB_PATH)
    keyword_trends = analyze_keyword_trends(df)
    category_trends = analyze_category_trends(df)
    author_trends = analyze_author_trends(df)

    logger.info(f"Keyword Trends: {keyword_trends}")
    logger.info(f"Category Trends: {category_trends}")
    logger.info(f"Author Trends: {author_trends}")

    logger.info("Finished testing.")


def test_simple_insert(db_path):
    try:
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO streamed_messages (message) VALUES ('Simple Test')")
            conn.commit()
            logger.info("Simple test insert complete")
            cursor.execute("SELECT COUNT(*) FROM streamed_messages")
            count = cursor.fetchone()[0]
            logger.info(f"Row count after simple insert {count}")
    except Exception as e:
        logger.exception(f"Error during simple insert: {e}")



if __name__ == "__main__":
    main()
