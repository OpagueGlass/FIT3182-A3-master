import os
from pymongo import MongoClient, ASCENDING, GEOSPHERE
import csv
from datetime import datetime


def main():
    ROOT_DIR = "/home/student"
    VEHICLE_FILENAME = f"{ROOT_DIR}/data/vehicle.csv"
    CAMERA_FILENAME = f"{ROOT_DIR}/data/camera.csv"
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://172.31.33.215:27017/")

    # Connect to MongoDB and get database reference
    mongo_client = MongoClient(MONGO_URI)

    db = mongo_client["fit3182_db"]

    # Drop existing collections to allow reruns
    db.vehicles.drop()
    db.cameras.drop()
    db.violations.drop()

    # Read from vehicle.csv, preparing each row of vehicle info for insertion as a document
    with open(VEHICLE_FILENAME, "r") as f:
        reader = csv.DictReader(f)

        unique_vehicles = {}

        for row in reader:
            car_plate = row["car_plate"]
            registration_date = datetime.fromisoformat(row["registration_date"])

            # If there are rows with the same car plate, insert version with latest registration date
            if car_plate not in unique_vehicles or registration_date > unique_vehicles[car_plate]["registration_date"]:
                unique_vehicles[car_plate] = {
                    "car_plate": car_plate,
                    "owner_name": row["owner_name"],
                    "owner_addr": row["owner_addr"],
                    "vehicle_type": row["vechicle_type"],
                    "registration_date": registration_date,
                }

    # JSON schema for validation rules of vehicles collection
    vehicle_schema = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["car_plate", "owner_name", "owner_addr", "vehicle_type", "registration_date"],
            "properties": {
                "car_plate": {"bsonType": "string"},
                "owner_name": {"bsonType": "string"},
                "owner_addr": {"bsonType": "string"},
                "vehicle_type": {"bsonType": "string"},
                "registration_date": {"bsonType": "date"},
            },
        }
    }

    # Create vehicles collection and insert each document
    vehicles = db.create_collection("vehicles", validator=vehicle_schema)
    result = vehicles.insert_many(unique_vehicles.values())
    result = vehicles.create_index([("car_plate", ASCENDING)], unique=True)

    # Read from camera.csv, preparing each row of camera info for insertion as a document
    with open(CAMERA_FILENAME, "r") as f:
        reader = csv.DictReader(f)

        camera_documents = []
        for row in reader:
            camera_documents.append(
                {
                    "camera_id": int(row["camera_id"]),
                    "location": {"type": "Point", "coordinates": [float(row["longitude"]), float(row["latitude"])]},
                    "position": float(row["position"]),
                    "speed_limit": int(row["speed_limit"]),
                }
            )

    # JSON schema for validation rules of cameras collection
    geoJSON_point_type = {
        "bsonType": "object",
        "required": ["type", "coordinates"],
        "properties": {
            "type": {"enum": ["Point"]},
            "coordinates": {"bsonType": "array", "items": {"bsonType": "double"}, "minItems": 2, "maxItems": 2},
        },
    }

    camera_schema = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["camera_id", "location", "position", "speed_limit"],
            "properties": {
                "camera_id": {"bsonType": "int"},
                "location": geoJSON_point_type,
                "position": {"bsonType": "double"},
                "speed_limit": {"bsonType": "int"},
            },
        }
    }

    # Create cameras collection and insert each document, then created indexes
    cameras = db.create_collection("cameras", validator=camera_schema)
    result = cameras.insert_many(camera_documents)
    result = cameras.create_index([("camera_id", ASCENDING)], unique=True)
    result = cameras.create_index([("location", GEOSPHERE)])

    # JSON schema for validation rules of violations collection
    violations_type = {
        "bsonType": "array",
        "items": {
            "bsonType": "object",
            "required": [
                "violation_type",
                "camera_id_start",
                "camera_id_end",
                "timestamp_start",
                "timestamp_end",
                "speed_reading",
            ],
            "properties": {
                "violation_type": {"enum": ["INSTANTANEOUS", "AVERAGE"]},
                "camera_id_start": {"bsonType": "int"},
                "camera_id_end": {"bsonType": "int"},
                "timestamp_start": {"bsonType": "date"},
                "timestamp_end": {"bsonType": "date"},
                "speed_reading": {"bsonType": "double"},
            },
        },
    }

    violation_schema = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["car_plate", "date", "violations"],
            "properties": {
                "car_plate": {"bsonType": "string"},
                "date": {"bsonType": "date"},
                "violations": violations_type,
            },
        }
    }

    # Create violations collection with validation rules and index
    violations = db.create_collection("violations", validator=violation_schema)
    result = violations.create_index([("car_plate", ASCENDING), ("date", ASCENDING)])
    mongo_client.close()


if __name__ == "__main__":
    main()
