import os
import dlt
import requests
from google.cloud import bigquery_storage
from google.cloud import bigquery
import pandas as pd
from io import BytesIO
from dlt.destinations import filesystem
import click

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
    pipeline_name="nyc_taxi_pipeline",
    destination="bigquery",
    dataset_name="nyc_taxi_dataset",
)

pipeline_gcs = dlt.pipeline(
    pipeline_name="nyc_taxi_pipeline",
    destination=filesystem(layout="{schema_name}/{table_name}.{ext}"),
    dataset_name="nyc_taxi_dataset",
)

# -----------------------------
# CLI entry: list of years and taxi types to fetch
# -----------------------------


@click.command()
@click.option(
    "--month",
    "single_month",
    type=int,
    default=None,
    help="Single month to fetch (1-12). Overrides --months",
)
@click.option(
    "--months",
    "months",
    multiple=True,
    type=str,
    default=("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"),
    help="Months to fetch. Use comma-separated values or repeat the option, e.g. --months 5,6,7 --months 8",
)
@click.option(
    "--years",
    "years",
    multiple=True,
    type=int,
    default=(2019, 2020),
    help="Years to fetch, e.g. --years 2019 2020",
)
@click.option(
    "--taxi",
    "taxi_types",
    multiple=True,
    type=click.Choice(["yellow", "green"], case_sensitive=False),
    default=("yellow", "green"),
    help="Taxi types to fetch, e.g. --taxi yellow green",
)
def main(single_month, months, years, taxi_types):
    # Normalize months: support repeated --months and comma-separated lists
    if single_month is not None:
        months_list = [int(single_month)]
    else:
        months_list = []
        for m in months:
            for part in str(m).split(","):
                part = part.strip()
                if part:
                    months_list.append(int(part))
    # validate and dedupe/sort
    months = sorted({m for m in months_list if 1 <= int(m) <= 12})
    years = list(years)
    taxi_types = [t.lower() for t in taxi_types]


    # -----------------------------
    # Process one month at a time
    # -----------------------------
    for year in years:
        for taxi in taxi_types:
            for month in months:
                url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi}_tripdata_{year}-{month:02d}.parquet"
                print(f"Fetching {taxi} taxi {year}-{month:02d}: {url}")

                # Download Parquet file
                response = requests.get(url)
                response.raise_for_status()
                df = pd.read_parquet(BytesIO(response.content))

                # Create a temporary dlt source for this single month
                @dlt.source(name=f"{taxi}_tripdata_{year}_dataset")
                def process_bigquery():
                    yield dlt.resource(df, name=f"{taxi}_tripdata_{year}")  # same table name for all months

                @dlt.source(name=f"{taxi}_tripdata_{year}")
                def process_gcs_bucket():
                    yield dlt.resource(df, name=f"{taxi}_tripdata_{year}_{month:02d}")

                # Run pipeline
                load_info = pipeline_bq.run(process_bigquery(), loader_file_format="parquet")
                print(f"{taxi} Taxi Year {year} Month {month} loaded. Info: {load_info}")

                load_info = pipeline_gcs.run(process_gcs_bucket(), loader_file_format="parquet")
                print(f"{taxi} Taxi Year {year} Month {month} loaded. Info: {load_info}")


if __name__ == "__main__":
    main()