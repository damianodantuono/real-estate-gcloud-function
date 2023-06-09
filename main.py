import functions_framework
import re
from google.cloud import bigquery
from google.cloud import storage


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data
    bucket = data["bucket"]
    name = data["name"]

    print("Triggered by a change in bucket: {}, file: {}".format(bucket, name))

    pattern = re.compile(r"raw_data/(\w+)/?(\w+)*/(\d{8})\.parquet")

    if match := pattern.match(name):
        city = match.group(1)
        zone = match.group(2)
        date = match.group(3)
        print("City: {}, Zone: {}, Date: {}".format(city, zone, date))
    else:
        raise ValueError("Cannot fetch city and date from file name")

    # fetch query from bucket
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(f"queries/{city}/query.sql")
    query = blob.download_as_string().decode("utf-8")

    # Set up BigQuery client
    client = bigquery.Client()
    client.query(query.format(CITY=city))
    print("Query executed successfully")
