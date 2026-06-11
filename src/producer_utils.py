import csv
import json
from collections import defaultdict
from datetime import datetime
import os
from time import sleep

from kafka3 import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "172.31.41.197:9092,172.31.43.191:9092")  # get Kafka bootstrap servers
ENCODING = "utf-8"  # Consistent encoding for reading CSV files and sending messages to Kafka
BATCH_SLEEP_SECONDS = 5  # Time to sleep between publishing batches
ROOT_DIR = "/home/student/"  # Root directory for docker container


def parse_row(row, producer_id):
    """
    Parses a CSV row into a dictionary with the expected fields for a camera event and converts them to their respective
    types. Gracefully handles invalid or malformed data by catching exceptions and logging the event data with an error
    message. Adds the producer_id and a Kafka timestamp to the event data as metadata for traceability and debugging.

    Input:
        - row: A dictionary representing a row from the CSV file, with keys corresponding to column names.
        - producer_id: The ID of the producer instance.
    Output:
        - A dictionary with the parsed, typed fields for a camera event, or None if parsing fails.
    """
    try:
        # Current timestamp for metadata
        current_timestamp = datetime.now().isoformat()

        # Extracts and converts fields from the CSV row
        event = {
            "event_id": row["event_id"],
            "batch_id": int(row["batch_id"]),
            "car_plate": row["car_plate"],
            "camera_id": int(row["camera_id"]),
            "timestamp": row["timestamp"],
            "speed_reading": float(row["speed_reading"]),
            "producer_id": producer_id,
            "kafka_timestamp": current_timestamp,
        }
    except Exception as e:
        print(f"Error parsing row {row}: {e}")
        return None

    return event


def publish_messages(producer_instance, topic_name, data):
    """
    Publishes a message to the specified Kafka topic using the provided producer instance. Handles exceptions
    that may occur during message sending and logs any errors encountered.

    Input:
        - producer_instance: An instance of KafkaProducer used to send messages to Kafka.
        - topic_name: The name of the Kafka topic to which the message should be sent.
        - data: The JSON event data to be sent as the message value
    Output:
        - None
    """
    try:
        producer_instance.send(topic_name, value=data)
    except Exception as e:
        print(f"Failed to send message: {e}")


def connect_kafka_producer():
    """
    Connects to the Kafka Producer using the specified bootstrap server and value serializer. Handles exceptions that
    may occur during connection and logs any errors encountered.

    Input:
        - None
    Output:
        - An instance of KafkaProducer if the connection is successful, or None if an exception occurs
    """
    producer = None
    try:
        # Connect to Kafka Producer with JSON serialization for message values using UTF-8 encoding
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
            value_serializer=lambda v: json.dumps(v).encode(ENCODING),
        )
    except Exception as e:
        print(f"Exception while connecting to Kafka: {e}")
    finally:
        return producer


def start_producer(filename, topic, producer_id):
    """ 
    Starts the Kafka producer by reading the specified CSV file, parsing the events, and publishing them to the given 
    Kafka topic in batches based on their batch_id. 
    
    Input:
        - filename: The name of the CSV file containing the camera events to be published.
        - topic: The name of the Kafka topic to which the events should be published.
        - producer_id: The ID of the producer instance, used for metadata in the event data
    Output:
        - None
    """
    # Read the CSV file and group events by batch_id.
    batches = defaultdict(list)
    with open(f"{ROOT_DIR}/data/{filename}", newline="", encoding=ENCODING) as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            event = parse_row(row, producer_id)
            if event:
                batch_id = event["batch_id"]
                batches[batch_id].append(event)

    producer = connect_kafka_producer()

    # Publish events to Kafka in batches based on batch_id every BATCH_SLEEP_SECONDS seconds
    for batch_id, batch in batches.items():
        for event in batch:
            publish_messages(producer, topic, event)
        producer.flush() # Ensure all messages in the batch are sent before sleeping
        sleep(BATCH_SLEEP_SECONDS)

    producer.close() # Close the producer connection after all batches have been published