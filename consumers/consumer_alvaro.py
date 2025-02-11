import json
import pathlib
import sys
import time
from tabulate import tabulate
import logging
import matplotlib.pyplot as plt
from collections import Counter

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# import from local modules
import utils.utils_config as config
from pymongo import MongoClient, errors


def process_message(message: str) -> dict:
    """
    Process and transform a single JSON message.
    Converts message fields to appropriate data types.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        processed_message = {
            "message": message.get("message"),
            "author": message.get("author"),
            "timestamp": message.get("timestamp"),
            "category": message.get("category"),
            "sentiment": float(message.get("sentiment", 0.0)),
            "keyword_mentioned": message.get("keyword_mentioned"),
            "message_length": int(message.get("message_length", 0)),
        }
        logger.info(f"Processed message: {processed_message}")
        return processed_message
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None


def consume_messages_from_file(live_data_path, mongo_uri, interval_secs, last_position):
    """
    Consume new messages from a file and process them.
    Each message is expected to be JSON-formatted.

    Args:
        live_data_path (pathlib.Path): Path to the live data file.
        mongo_uri (str): Connection URI for MongoDB.
        interval_secs (int): Interval in seconds to check for new messages.
        last_position (int): Last read position in the file.
    """
    logger.info("Called consume_messages_from_file() with:")
    logger.info(f"   live_data_path={live_data_path}")
    logger.info(f"   mongo_uri={mongo_uri}")
    logger.info(f"   interval_secs={interval_secs}")
    logger.info(f"   last_position={last_position}")

    logger.info("1. Initialize the MongoDB connection.")
    collection = init_db(mongo_uri)

    logger.info("2. Set the last position to 0 to start at the beginning of the file.")
    last_position = 0
    all_messages = []

    while True:
        try:
            logger.info(f"3. Read from live data file at position {last_position}.")
            with open(live_data_path, "r") as file:
                file.seek(last_position)
                for line in file:
                    if line.strip():
                        message = json.loads(line.strip())
                        processed_message = process_message(message)
                        if processed_message:
                            insert_message(processed_message, collection)
                            all_messages.append(processed_message)

                last_position = file.tell()
                if all_messages:
                    display_message_table(all_messages)
                    all_messages = []

        except FileNotFoundError:
            logger.error(f"ERROR: Live data file not found at {live_data_path}.")
            sys.exit(10)
        except Exception as e:
            logger.error(f"ERROR: Error reading from live data file: {e}")
            sys.exit(11)

        time.sleep(interval_secs)


def main():
    """
    Main function to run the consumer process.
    Reads configuration, initializes the database, and starts consumption.
    """
    logger.info("Starting Consumer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")

    logger.info("STEP 1. Read environment variables using new config functions.")
    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        live_data_path: pathlib.Path = config.get_live_data_path()
        mongo_uri: str = config.get_mongo_uri()
        logger.info("SUCCESS: Read environment variables.")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 4. Begin consuming and storing messages.")
    try:
        consume_messages_from_file(live_data_path, mongo_uri, interval_secs, 0)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        logger.info("TRY/FINALLY: Consumer shutting down.")


def init_db(mongo_uri):
    """Initializes the MongoDB connection."""
    try:
        client = MongoClient(mongo_uri)
        db = client["product_launch_mongodb"]  # Replace with your database name
        collection = db["messages"]  # Replace with your collection name
        return collection
    except errors.ConnectionFailure as e:
        logger.error(f"Could not connect to MongoDB: {e}")
        sys.exit(1)


def insert_message(message, collection):
    """Inserts a message into the MongoDB collection."""
    try:
        collection.insert_one(message)
        logger.info(f"Inserted message: {message}")
    except errors.PyMongoError as e:
        logger.error(f"Error inserting message: {e}")


def display_message_table(messages):
    """Displays a table of messages using tabulate."""
    if messages:
        print("\n--- Messages Processed ---\n")
        print(tabulate(messages, headers="keys"))
        print("--- End of Messages ---\n")
        create_bar_chart(messages)


def create_bar_chart(messages):
    """Creates and displays a bar chart of message categories."""
    categories = [msg['category'] for msg in messages if 'category' in msg]
    category_counts = Counter(categories)

    categories_list = list(category_counts.keys())
    counts_list = list(category_counts.values())

    plt.figure(figsize=(10, 6))  # Adjust figure size as needed
    plt.bar(categories_list, counts_list, color='skyblue')
    plt.xlabel("Category")
    plt.ylabel("Number of Messages")
    plt.title("Alvaro Quintero's Bar Chart")
    plt.xticks(rotation=45, ha="right")  # Rotate x-axis labels for readability
    plt.tight_layout()  # Adjust layout
    plt.show() # Added to display the chart


if __name__ == "__main__":
    main()
