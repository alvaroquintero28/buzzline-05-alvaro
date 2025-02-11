import json
import time
from kafka import KafkaProducer
import random
import datetime
import pathlib

from utils.utils_config import (
    get_kafka_broker_address,
    get_kafka_topic,
    get_message_interval_seconds,
    get_base_data_path
)

from utils.utils_logger import logger

def main():
    logger.info("Starting Producer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")
    logger.info("STEP 1. Read required environment variables.")

    try:
        KAFKA_BROKER_ADDRESS = get_kafka_broker_address()
        BUZZ_TOPIC = get_kafka_topic()
        MESSAGE_INTERVAL_SECONDS = get_message_interval_seconds()
        BASE_DATA_DIR = get_base_data_path()

        logger.info("STEP 2. Initialize Kafka Producer.")
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER_ADDRESS],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
        )

        logger.info("STEP 3. Start producing messages continuously.")
        while True:
            try:
                message = generate_random_message()
                producer.send(BUZZ_TOPIC, key="key", value=message)
                logger.info(f"Sent message: {message}")
                time.sleep(MESSAGE_INTERVAL_SECONDS)

            except Exception as e:
                logger.exception(f"An error occurred while producing a message: {e}")
                time.sleep(10)  # Pause before trying again

    except Exception as e:
        logger.exception(f"A critical error occurred: {e}")
    finally:
        if 'producer' in locals() and producer is not None:
            producer.close()
        logger.info("Producer closed gracefully.")


def generate_random_message() -> dict:
    """Generates a random message with various fields."""
    categories = ["News", "Sports", "Tech", "Politics", "Finance"]
    keywords = ["election", "crypto", "soccer", "inflation", "AI"]

    message = {
        "message": f"This is a random message: {random.randint(1, 1000)}",
        "author": f"User{random.randint(1, 10)}",
        "timestamp": datetime.datetime.now().isoformat(),
        "category": random.choice(categories),
        "sentiment": random.uniform(-1.0, 1.0),  # Sentiment from -1 to 1
        "keyword_mentioned": random.choice(keywords),
        "message_length": len(message["message"]),
    }
    return message


if __name__ == "__main__":
    main()

