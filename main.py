import functions_framework
from google.cloud import bigquery
from google.cloud import storage
from pathlib import Path


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def file_processing(cloud_event):
    data = cloud_event.data
    bucket = data["bucket"]
    name = data["name"]
    filename = Path(name).name

    DESTINATION_BUCKET = 'archived-scraped-data'

    print("Triggered by a change in bucket: {}, file: {}".format(bucket, name))

    client = bigquery.Client()

    with open("sql_scripts/load_to_ext.sql", "r") as f:
        load_to_ext_query = f.read().format(bucket=bucket, name=name)

    _ = client.query(load_to_ext_query).result()

    print("loaded to ext table")

    with open("sql_scripts/update_dim_city.sql", "r") as f:
        update_dim_city_query = f.read()

    _ = client.query(update_dim_city_query).result()

    print("updated dim city table")

    with open("sql_scripts/insert_into_fct.sql", "r") as f:
        insert_into_fct_query = f.read()

    _ = client.query(insert_into_fct_query).result()

    print("inserted into fct table")

    client = storage.Client()
    source_bucket = client.get_bucket(bucket)
    destination_bucket = client.get_bucket(DESTINATION_BUCKET)
    blob = source_bucket.blob(name)

    print(f"Archiving {filename} to {DESTINATION_BUCKET}")

    _ = source_bucket.copy_blob(blob, destination_bucket, filename)
    blob.delete()
