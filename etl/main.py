import os
import json
import requests
from datetime import datetime
from google.cloud import storage, bigquery
import functions_framework

NASA_API_KEY = os.environ.get("NASA_API_KEY")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
BQ_PROJECT = os.environ.get("BQ_PROJECT")
BQ_DATASET = "nasa_neo"
BQ_TABLE = "raw_neo_feed"


def fetch_nasa_data():
    """Call NASA NEO API for today's date"""
    today = datetime.utcnow().date()
    url = (
        "https://api.nasa.gov/neo/rest/v1/feed"
        f"?start_date={today}&end_date={today}&api_key={NASA_API_KEY}"
    )
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json(), today


def save_raw_to_gcs(data, date):
    """Save raw JSON response in Cloud Storage"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    blob_path = f"raw/neo/{date.year}/{date.month:02d}/{date.day:02d}/neo_{date}.json"
    blob = bucket.blob(blob_path)
    blob.upload_from_string(json.dumps(data), content_type="application/json")

    print(f"Raw JSON saved to gs://{BUCKET_NAME}/{blob_path}")


def transform_records(data, date):
    """Flatten NASA NEO API JSON structure into rows for BigQuery"""
    records = []

    neos = data["near_earth_objects"].get(str(date), [])

    for obj in neos:
        # Tomamos el primer "close_approach_data"
        if not obj["close_approach_data"]:
            continue
        approach = obj["close_approach_data"][0]

        record = {
            "asteroid_id": obj["neo_reference_id"],
            "name": obj["name"],
            "absolute_magnitude_h": float(obj["absolute_magnitude_h"]),
            "estimated_diameter_min_km": float(
                obj["estimated_diameter"]["kilometers"]["estimated_diameter_min"]
            ),
            "estimated_diameter_max_km": float(
                obj["estimated_diameter"]["kilometers"]["estimated_diameter_max"]
            ),
            "is_potentially_hazardous": obj["is_potentially_hazardous_asteroid"],
            "close_approach_date": approach["close_approach_date"],
            "relative_velocity_km_s": float(
                approach["relative_velocity"]["kilometers_per_second"]
            ),
            "miss_distance_km": float(approach["miss_distance"]["kilometers"]),
            "orbiting_body": approach["orbiting_body"],
            "ingestion_timestamp": datetime.utcnow().isoformat(),
        }

        records.append(record)

    print(f"Transformed {len(records)} records")
    return records


def load_to_bigquery(records):
    """Insert flattened rows into BigQuery"""
    client = bigquery.Client(project=BQ_PROJECT)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    errors = client.insert_rows_json(table_id, records)
    if errors:
        print("Errors occurred:", errors)
    else:
        print(f"Inserted {len(records)} rows into {table_id}")


@functions_framework.http
def etl_nasa_neo(request):
    """HTTP entrypoint for Cloud Run / Scheduler"""
    print("Starting NASA NEO ETL...")

    if not (NASA_API_KEY and BUCKET_NAME and BQ_PROJECT):
        msg = "Missing required environment variables"
        print(msg)
        return (msg, 500)

    data, date = fetch_nasa_data()
    save_raw_to_gcs(data, date)

    records = transform_records(data, date)

    if records:
        load_to_bigquery(records)

    response_body = {
        "status": "success",
        "rows_inserted": len(records),
        "date": str(date),
    }
    return (json.dumps(response_body), 200, {"Content-Type": "application/json"})
