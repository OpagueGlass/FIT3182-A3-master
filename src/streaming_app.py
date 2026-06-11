import os

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages "
    "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 "
    "pyspark-shell"
)

from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
from pyspark.sql.functions import col, from_json, to_timestamp, expr, udf, lit
from datetime import datetime
from time import sleep
from logging import getLogger, INFO, Formatter
from logging.handlers import RotatingFileHandler
import shutil

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "172.31.41.197:9092,172.31.43.191:9092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://172.31.33.215:27017/")
TOPIC_A = "camera-events-A"
TOPIC_B = "camera-events-B"
TOPIC_C = "camera-events-C"
HOURS_TO_SECONDS = 3600
MAX_WRITE_RETRIES = 3
RETRY_DELAY_SECONDS = 2

# Define file paths for logging dropped pairs and checkpoint locations
ROOT_DIR = "/home/student"
CHECKPOINT_DIR = f"{ROOT_DIR}/checkpoints"
CHECKPOINT_INSTANT_A = f"{CHECKPOINT_DIR}/instant_a"
CHECKPOINT_INSTANT_B = f"{CHECKPOINT_DIR}/instant_b"
CHECKPOINT_INSTANT_C = f"{CHECKPOINT_DIR}/instant_c"
CHECKPOINT_AVERAGE_AB = f"{CHECKPOINT_DIR}/average_ab"
CHECKPOINT_AVERAGE_BC = f"{CHECKPOINT_DIR}/average_bc"
CHECKPOINT_UNMATCHED_AB = f"{CHECKPOINT_DIR}/unmatched_ab"
CHECKPOINT_UNMATCHED_BC = f"{CHECKPOINT_DIR}/unmatched_bc"


def main():
    # Remove checkpoints from previous runs
    if os.path.exists(CHECKPOINT_DIR):
        shutil.rmtree(CHECKPOINT_DIR)
        
    # Initialise MongoDB client and database
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client["fit3182_db"]

    # Retrieve camera details in order of camera_id
    cameras = db.cameras.find({}, {"_id": 0, "camera_id": 1, "speed_limit": 1, "position": 1}).sort("camera_id", 1)

    # Create speed limits and camera positions dictionaries by camera id
    speed_limits = {}
    camera_positions = {} 

    for each_camera in cameras:
        camera_id = each_camera["camera_id"]
        speed_limit = each_camera["speed_limit"]
        position = each_camera["position"]

        speed_limits[camera_id] = speed_limit
        camera_positions[camera_id] = position
        
    camera_times = {}
    camera_distances = {}
    camera_ids = list(speed_limits.keys())

    for index in range(1, len(camera_ids)):
        current_speed_limit = speed_limits[camera_ids[index]]
        previous_position = camera_positions[camera_ids[index - 1]]
        current_position = camera_positions[camera_ids[index]]

        # Calculate distance between adjacent cameras in kilometers with their Kilometer-markers on the highway
        distance = current_position - previous_position
        camera_distances[(camera_ids[index - 1], camera_ids[index])] = distance

        # Compute time between adjacent cameras and convert hours to seconds, rounding to nanoseconds
        camera_times[(camera_ids[index - 1], camera_ids[index])] = round(
            distance / (current_speed_limit / HOURS_TO_SECONDS), 9
        )
        
    
    # Initialise the Spark Session with all available local cores and stopping gracefully on shutdown
    spark = (
        SparkSession.builder
        .master(os.getenv("SPARK_MASTER_URL", "spark://172.31.40.21:7077"))
        .appName("FIT3182 A2 Streaming Join")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    
    # Define the schema to parse the incoming JSON events from Kafka based on the format in the producers
    event_schema = StructType([
        StructField("event_id", StringType()),
        StructField("batch_id", IntegerType()),
        StructField("car_plate", StringType()),
        StructField("camera_id", IntegerType()),
        StructField("timestamp", StringType()),
        StructField("speed_reading", DoubleType()),
        StructField("producer_id", StringType()),
        StructField("kafka_timestamp", StringType())
    ])
    
    def read_camera_stream(topic_name, watermark_delay):
        """
        Reads a JSON camera event stream from a Kafka topic and applies a watermark based on the provided delay.
        
        Inputs:
            - topic_name: The name of the Kafka topic to subscribe to for the camera events.
            - watermark_delay: The delay threshold as an interval for the watermark.
        Output:
            Returns the camera event stream as a streaming DataFrame with the event_time and processing_time columns with 
            the watermark applied.
        """
        return (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", topic_name)
            .option("startingOffsets", "latest") # Start from latest offsets to only process new events
            .option("failOnDataLoss", "false") # Continue processing even if some data is lost
            .load() # Loads the streaming data from Kafka as a DataFrame
            .selectExpr("CAST(value AS STRING) AS json_value") # Extract the JSON string from the binary value column
            .select(from_json(col("json_value"), event_schema).alias("data")) # Parse the JSON string based on the defined schema into a structured column
            .select("data.*")
            .withColumn("event_time", to_timestamp(col("timestamp")))
            .withColumn("processing_time", to_timestamp(col("kafka_timestamp"))) 
            .withWatermark("event_time", watermark_delay)
        )
    
    # Watermark delays based on largest expected time difference from the maximum timestamp, obtained from watermark_analysis.ipynb
    camera_a_stream = read_camera_stream(TOPIC_A, "5 seconds")
    camera_b_stream = read_camera_stream(TOPIC_B, "11 minutes")
    camera_c_stream = read_camera_stream(TOPIC_C, "31 minutes")
    
    # Get the time and distances for joining the camera streams based on their adjacent camera pairs
    camera_times_ab = camera_times[(camera_ids[0], camera_ids[1])]
    camera_times_bc = camera_times[(camera_ids[1], camera_ids[2])]

    camera_distances_ab = camera_distances[(camera_ids[0], camera_ids[1])]
    camera_distances_bc = camera_distances[(camera_ids[1], camera_ids[2])]


    def join_streams(stream1, stream2, time):
        """
        Performs a left outer join between two camera event streams based on the car plate where the event time of the 
        second stream is greater than the first stream's event time, and their difference is strictly below the 
        specified time interval required to travel between the cameras at the speed limit of the ending camera. Columns are
        selected and renamed for the output.
        
        Input:
            - stream1: The first camera event stream as a streaming DataFrame to join.
            - stream2: The second camera event stream as a streaming DataFrame to join.
            - time: The time interval in seconds required to travel between the cameras at the speed limit of the ending 
            camera used for the join condition.
        Output:
            Returns a streaming DataFrame resulting from the left outer join of the two input streams based on the join
            condition, with the relevant columns selected and renamed.
        """
        
        join_expr = expr(
            f"""
            s1.car_plate = s2.car_plate AND
            s1.event_time < s2.event_time AND
            s2.event_time < s1.event_time + INTERVAL {time} SECONDS
            """
        )

        return (
            stream1.alias("s1")
            .join(stream2.alias("s2"), join_expr, how="leftOuter")
            .select(
                col("s1.car_plate").alias("car_plate"),
                col("s1.camera_id").alias("camera_id_start"),
                col("s2.camera_id").alias("camera_id_end"),
                col("s1.event_time").alias("timestamp_start"),
                col("s2.event_time").alias("timestamp_end"),
                col("s1.speed_reading").alias("speed_start"),
                col("s2.speed_reading").alias("speed_end"),
                col("s1.producer_id").alias("producer_id"),
                col("s1.processing_time").alias("processing_time")
            )
        )
        
    # Apply the join to the adjacent camera streams
    joined_ab = join_streams(camera_a_stream, camera_b_stream, camera_times_ab)
    joined_bc = join_streams(camera_b_stream, camera_c_stream, camera_times_bc)
    
    def filter_unmatched_pairs(joined_stream):
        """
        Filter unmatched pairs from the joined stream. Checks for null values in the camera_id_end column to identify 
        unmatched events from the starting camera and selects the relevant columns for the output.
        
        Input:
            joined_stream: The joined camera event stream as a streaming DataFrame to filter for unmatched events from the
            starting camera.
        Output:
            Returns a streaming DataFrame containing the unmatched events from the starting camera
        """
        return joined_stream.filter(col("camera_id_end").isNull()).select(
            col("car_plate"),
            col("camera_id_start"),
            col("timestamp_start"),
            col("producer_id"),
            col("processing_time")
            )

    def filter_matched_pairs(joined_stream):
        """
        Filter matched pairs from the joined stream. Checks for non-null values in the camera_id_end column to identify 
        matched events from the starting and ending cameras and selects the relevant columns for the output.
        
        Input:
            joined_stream: The joined camera event stream as a streaming DataFrame to filter for matched events from the 
            starting and ending cameras.
        Output:
            Returns a streaming DataFrame containing the matched events from the starting and ending cameras.
        """
        return joined_stream.filter(col("camera_id_end").isNotNull()).select(
            col("car_plate"),
            col("camera_id_start"),
            col("camera_id_end"),
            col("timestamp_start"),
            col("timestamp_end"),
            col("speed_start"),
            col("speed_end"),
        )

    # Filter the joined streams for unmatched and matched pairs for both adjacent camera pairs
    unmatched_ab = filter_unmatched_pairs(joined_ab)
    unmatched_bc = filter_unmatched_pairs(joined_bc)

    inner_join_ab = filter_matched_pairs(joined_ab)
    inner_join_bc = filter_matched_pairs(joined_bc)
    

    # Filter the joined streams for unmatched and matched pairs for both adjacent camera pairs
    unmatched_ab = filter_unmatched_pairs(joined_ab)
    unmatched_bc = filter_unmatched_pairs(joined_bc)

    inner_join_ab = filter_matched_pairs(joined_ab)
    inner_join_bc = filter_matched_pairs(joined_bc)
    
    def log_dropped_pairs(batch_df, batch_id):
        """
        Logs the unmatched events from the starting camera as dropped pairs. This function is used as a foreachBatch sink in
        the streaming query for unmatched pairs to log the dropped pairs after the watermark delay has passed.
        
        Records can be unmatched due to the following reasons:
        - The car only appears in the starting camera and not the ending camera
        - The car appears in the ending camera very late outside of the watermark delay
        - The car does not exceed the speed limit of the ending camera and is not detected as a match in the joined stream
        """
        if not batch_df.isEmpty():
            # Only show if there are unmatched events in the batch
            batch_df.show(truncate=False)
            
    def is_speeding(camera_id, speed):
        """
        Checks if the given speed exceeds the speed limit of the camera
        
        Inputs:
            - camera_id: The ID of the camera which recorded the speed reading.
            - speed: The speed reading to check against the camera's speed limit.
        Output:
            Returns True if the speed exceeds the camera's speed limit, False otherwise.
        """
        return speed > speed_limits.get(camera_id, float("inf"))

    # Create a user defined function (UDF) to apply the is_speeding function to the streaming DataFrames for filtering
    is_speeding_udf = udf(is_speeding, BooleanType())

    def instant_violation(camera_stream):
        """
        Filters the given streaming DataFrame for events where the speed reading exceeds the camera's speed limit and 
        selects and renames the relevant columns for the output.
        
        Input:
            - camera_stream: The camera event stream as a streaming DataFrame to filter for instantaneous speed violations.
        Output:
            Returns a streaming DataFrame containing only the events where the speed reading exceeds the camera's speed 
            limit, with the relevant columns selected and renamed and a violation type of "INSTANTANEOUS" added as a new 
            column.
        """
        return camera_stream.filter(is_speeding_udf(col("camera_id"), col("speed_reading"))).select(
            col("car_plate"),
            col("camera_id").alias("camera_id_start"),
            col("camera_id").alias("camera_id_end"),
            col("event_time").alias("timestamp_start"),
            col("event_time").alias("timestamp_end"),
            col("speed_reading"),
            lit("INSTANTANEOUS").alias("violation_type"),
        )
        
    # Apply the instant_violation function to each camera stream to filter for instantaneous speed violations
    instant_violations_a = instant_violation(camera_a_stream)
    instant_violations_b = instant_violation(camera_b_stream)
    instant_violations_c = instant_violation(camera_c_stream)
    
    def timestamp_diff(timestamp_start, timestamp_end):
        """
        Calculates the difference in seconds between two timestamps with microsecond precision. This function is used 
        instead of unix_timestamp since unix_timestamp truncates the timestamps to seconds and removes the microsecond
        precision, leading to inaccurate average speed calculations for short time intervals between adjacent cameras.
        
        Inputs:
            - timestamp_start: The starting timestamp to calculate the difference from.
            - timestamp_end: The ending timestamp to calculate the difference to.
        Output:
            Returns the difference in seconds between the two timestamps as a float, including microsecond precision.
        """
        return (timestamp_end - timestamp_start).total_seconds()

    # Create a UDF to apply the timestamp_diff function to the streaming DataFrames for calculating time differences
    timestamp_diff_udf = udf(timestamp_diff, DoubleType())


    def add_avg_speed(inner_join_stream, distance_km):
        """
        Calculates the average speed in km/h by dividing the distance between the cameras in kilometers by the time 
        difference in hours between the timestamps of the starting and ending camera events, and adds the average speed 
        reading and the violation type of "AVERAGE" as new columns to the given streaming DataFrame.
        
        Input:
            - inner_join_stream: The streaming DataFrame containing the matched events to calculate the average speed for.
            - distance_km: The distance in kilometers between the starting and ending cameras to use for calculating the 
            average speed.
        Output:
            Returns the given streaming DataFrame with the speed_reading column containing the calculated average speed in 
            km/h, and violation_type of "AVERAGE"
        """
        return inner_join_stream.withColumn(
            "speed_reading",
            lit(distance_km) / (timestamp_diff_udf(col("timestamp_start"), col("timestamp_end")) / lit(HOURS_TO_SECONDS)),
        ).withColumn("violation_type", lit("AVERAGE"))

    # Apply the add_avg_speed function to the joined streams of matched pairs
    inner_join_ab = add_avg_speed(inner_join_ab, camera_distances_ab)
    inner_join_bc = add_avg_speed(inner_join_bc, camera_distances_bc)
    
    def update_operation(row):
        """
        Constructs a UpdateOne operation for a single violation row. Uses upsert to
        either insert a new daily violation record for the vehicle or append the violation to
        an existing one, ensuring violations for the same car on the same date are merged into
        a single document.

        Input:
            - row: A Spark Row object containing car_plate, violation_type, camera_id_start,
            camera_id_end, timestamp_start, timestamp_end, and speed_reading.
        Output:
            Returns a UpdateOne operation to be passed to bulk_write().
        """
        timestamp_start = row.timestamp_start
        violation_date = datetime(timestamp_start.year, timestamp_start.month, timestamp_start.day)
        violation_doc = {
            "violation_type": row.violation_type,
            "camera_id_start": row.camera_id_start,
            "camera_id_end": row.camera_id_end,
            "timestamp_start": timestamp_start,
            "timestamp_end": row.timestamp_end,
            "speed_reading": row.speed_reading,
        }
        update = UpdateOne(
            filter={"car_plate": row.car_plate, "date": violation_date},
            update={
                # Ensures car_plate and date are only written on document creation
                "$setOnInsert": {
                    "car_plate": row.car_plate,
                    "date": violation_date,
                },
                # Ensures duplicate violation documents are not added if the batch is retried
                "$addToSet": {"violations": violation_doc},
            },
            upsert=True,
        )
        return update


    def write_to_mongo(batch_df, batch_id):
        """
        Writes a micro-batch of detected violations to the MongoDB violations collection
        using bulk upserts. 

        Failed writes are retried up to MAX_WRITE_RETRIES times with a delay of
        RETRY_DELAY_SECONDS between attempts. ordered=False allows independent operations
        to continue even if one fails.

        Inputs:
            - batch_df: A Spark DataFrame containing the rows of the current micro-batch.
            - batch_id: The identifier of the current micro-batch, provided by foreachBatch.
        Output:
            None. Writes violation records to MongoDB in place.
        """
        
        rows = batch_df.collect()
        if not rows:
            return

        update_operations = [update_operation(each_row) for each_row in rows]

        violations = db.violations

        # Handle failed writes with retry handling
        for attempt_number in range(MAX_WRITE_RETRIES):
            try:
                violations.bulk_write(update_operations, ordered=False)
                return
            except Exception:
                if attempt_number < MAX_WRITE_RETRIES - 1:
                    sleep(RETRY_DELAY_SECONDS)


    def start_stream(stream, checkpoint_location, batch_function=write_to_mongo):
        """
        Starts a Spark Structured Streaming query in append output mode, writing each
        micro-batch using the provided batch function. Checkpointing is enabled to allow
        the query to resume from where it left off if the application restarts.

        Inputs:
            - stream: The streaming DataFrame to write.
            - checkpoint_location: File path for storing the streaming checkpoint state.
            - batch_function: The foreachBatch sink function to call per micro-batch.
            Defaults to write_to_mongo.
        Output:
            Returns the started StreamingQuery object.
        """
        return (
            stream.writeStream.foreachBatch(batch_function)
            .outputMode("append").option("checkpointLocation", checkpoint_location)
            .start()
        )
        
    try:
        # Start the streaming queries for the instantaneous violations, average speed violations, and unmatched pairs to log
        # dropped pairs
        query_a = start_stream(instant_violations_a, CHECKPOINT_INSTANT_A)
        query_b = start_stream(instant_violations_b, CHECKPOINT_INSTANT_B)
        query_c = start_stream(instant_violations_c, CHECKPOINT_INSTANT_C)
        query_ab = start_stream(inner_join_ab, CHECKPOINT_AVERAGE_AB)
        query_bc = start_stream(inner_join_bc, CHECKPOINT_AVERAGE_BC)
        log_unmatched_ab = start_stream(unmatched_ab, CHECKPOINT_UNMATCHED_AB, log_dropped_pairs)
        log_unmatched_bc = start_stream(unmatched_bc, CHECKPOINT_UNMATCHED_BC, log_dropped_pairs)
        print("Streaming queries started. Awaiting termination...")
        print("Logging dropped pairs to console:")
        spark.streams.awaitAnyTermination() # Wait for any of the streaming queries to terminate
    except Exception as e:
        print(f"Exception: {e}")
    finally:
        # Stop all streaming queries and close the MongoDB client connection on termination
        query_a.stop()
        query_b.stop()
        query_c.stop()
        query_ab.stop()
        query_bc.stop()
        log_unmatched_ab.stop()
        log_unmatched_bc.stop()
        spark.streams.resetTerminated() # Reset the terminated state of the streaming queries to allow for restarting
        mongo_client.close()
        
if __name__ == "__main__":
    main()