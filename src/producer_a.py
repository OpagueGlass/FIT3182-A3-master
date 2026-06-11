from producer_utils import start_producer
FILENAME_A = "camera_event_A.csv"
PRODUCER_ID_A = "A"
TOPIC_A = "camera-events-A"
    
if __name__ == "__main__":
    start_producer(FILENAME_A, TOPIC_A, PRODUCER_ID_A)