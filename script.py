import requests
from google.cloud import storage, bigquery
from google.cloud import exceptions as gc_exceptions
import json
import time
import pandas as pd

API_KEY = ""
ENDPOINT = "https://financialmodelingprep.com/api/v3/historical-price-full"
STOCKS = ["SPY"]

# GOOGLE CLOUD SETTINGS
CLOUD_PROJECT_ID = ""
CLOUD_STORAGE_BUCKET_NAME = ""
CLOUD_STORAGE_BUCKET_REGION = ""

CLOUD_BIGQUERY_DATASET_NAME = ""
CLOUD_BIGQUERY_TABLE_NAME = ""


def get_historical_data(url):
    print("Getting historical data")
    response = requests.get(url)
    return response.json()

def upload_to_bucket(bucket, data):
    try:
        print("Uploading data to bucket")
        df = pd.read_json(data)
        csv_data = df.to_csv(index=False)
        filename = str(int(time.time())) + ".csv"
        blob = bucket.blob(filename)
        blob.upload_from_string(data=csv_data, content_type='text/csv')
        print("Uploaded {} to {}".format(filename, CLOUD_STORAGE_BUCKET_NAME))
        return f"gs://{CLOUD_STORAGE_BUCKET_NAME}/{filename}"
    except Exception as e:
        print(f"{e}")

def process_data(data):
    print("Processing data")
    processed_data = []
    
    if "historicalStockList" in data:
        for records in data["historicalStockList"]:
            for record in records["historical"]:
                record["symbol"] = records["symbol"]
                processed_data.append(record)
    else:
        for record in data["historical"]:
            record["symbol"] = data["symbol"]
            processed_data.append(record)

    return processed_data

def load_file_to_bigquery(uri):
    print("Loading data to BigQuery")
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
    )
    job_config.autodetect = True
    table_id = f"{CLOUD_PROJECT_ID}.{CLOUD_BIGQUERY_DATASET_NAME}.{CLOUD_BIGQUERY_TABLE_NAME}"
    try:
        load_job = client.load_table_from_uri(
            uri,
            table_id,
            location="US",  # Must match the destination dataset location.
            job_config=job_config,
        )  # Make an API request.

        load_job.result()  # Waits for the job to complete.

        destination_table = client.get_table(table_id)
        print("Loaded {} rows.".format(destination_table.num_rows))
    except Exception as e:
        print(e)
        print(load_job.errors)


if __name__ == "__main__":
    url = f"{ENDPOINT}/{','.join(STOCKS)}?serietype=line&apikey={API_KEY}"
    data = get_historical_data(url)
    cleaned_data = process_data(data)
    
    storage_client = storage.Client()
    try:
        bucket = storage_client.get_bucket(CLOUD_STORAGE_BUCKET_NAME)
    except gc_exceptions.NotFound:
        print("Sorry, that bucket does not exist!")
        exit()
    
    uri = upload_to_bucket(bucket, json.dumps(cleaned_data))
    load_file_to_bigquery(uri)
    
    

    