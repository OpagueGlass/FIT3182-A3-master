from producer_utils import start_producer
FILENAME_C = "camera_event_C.csv"
PRODUCER_ID_C = "C"
TOPIC_C = "camera-events-C"

if __name__ == "__main__":
    start_producer(FILENAME_C, TOPIC_C, PRODUCER_ID_C)