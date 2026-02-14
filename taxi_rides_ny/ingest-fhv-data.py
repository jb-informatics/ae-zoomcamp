import os
import dlt
import requests
import pandas as pd
from google.cloud import bigquery_storage
from google.cloud import bigquery
from io import BytesIO
from dlt.destinations import filesystem

PROJECT_ROOT = os.getcwd()
KEYS_PATH = os.path.join(PROJECT_ROOT, "keys.json")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEYS_PATH
os.environ["DESTINATION__BIGQUERY__LOCATION"] = "US"
os.environ["BUCKET_URL"] = "gs://de-zoomcamp-terraform-484919-data-lake"

print("Using credentials:", os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

client = bigquery.Client()
print("Authenticated project:", client.project)

# -----------------------------
# Create the pipeline
# -----------------------------
pipeline_bq = dlt.pipeline(
    pipeline_name="fhv_to_bigquery",
    destination="bigquery",
    dataset_name="nyc_taxi_dataset",
)

pipeline_gcs = dlt.pipeline(
    pipeline_name="fhv_to_gcs",
    destination=filesystem(layout="{schema_name}/{table_name}.{ext}"),
    dataset_name="nyc_taxi_dataset",
)

# -----------------------------
# List of months to fetch
# -----------------------------
months = range(1, 13)

# -----------------------------
# Process one month at a time
# -----------------------------
for month in months:
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-{month:02d}.parquet"
    print(f"Fetching month {month}: {url}")

    # Download CSV file
    response = requests.get(url)
    response.raise_for_status()
    df = pd.read_parquet(BytesIO(response.content))

    # Fix data types for BigQuery compatibility
    df = df.astype({
        'PUlocationID': 'float32',
        'DOlocationID': 'float32'
    })

    # Create a temporary dlt source for this single month
    @dlt.source(name=f"fhv_tripdata_2019_dataset")
    def process_bigquery():
        yield dlt.resource(df, name="fhv_tripdata_2019")  # same table name for all months

    @dlt.source(name=f"fhv_tripdata_2019")
    def process_gcs_bucket():
        yield dlt.resource(df, name=f"fhv_tripdata_2019_{month:02d}")

    # Run pipeline
    load_info = pipeline_bq.run(process_bigquery(), loader_file_format="parquet")
    print(f"Month {month} loaded. Info: {load_info}")
    
    load_info = pipeline_gcs.run(process_gcs_bucket(), loader_file_format="parquet")
    print(f"Month {month} loaded. Info: {load_info}")