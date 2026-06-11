from producer_utils import start_producer
FILENAME_B = "camera_event_B.csv"
PRODUCER_ID_B = "B"
TOPIC_B = "camera-events-B"
        
if __name__ == "__main__":
    start_producer(FILENAME_B, TOPIC_B, PRODUCER_ID_B)