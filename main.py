import functions_framework

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")



# import functions_framework
# from google.cloud import bigquery
# from google.cloud import storage
#
#
# # Triggered by a change in a storage bucket
# @functions_framework.cloud_event
# def file_processing(cloud_event):
#     data = cloud_event.data
#     bucket = data["bucket"]
#     name = data["name"]
#
#     DESTINATION_BUCKET = 'archived-scraped-data'
#
#     print("Triggered by a change in bucket: {}, file: {}".format(bucket, name))
#
#     # client = bigquery.Client()
#     #
#     # with open("sql_scripts/load_to_ext.sql.sql", "r") as f:
#     #     load_to_ext_query = f.read()
#     #
#     # client.query(load_to_ext_query)
#     #
#     # print("loaded to ext table")
#     #
#     # with open("sql_scripts/update_dim_city.sql", "r") as f:
#     #     update_dim_city_query = f.read()
#     #
#     # client.query(update_dim_city_query)
#     #
#     # print("updated dim city table")
#     #
#     # with open("sql_scripts/insert_into_fct.sql", "r") as f:
#     #     insert_into_fct_query = f.read()
#     #
#     # client.query(insert_into_fct_query)
#     #
#     # print("inserted into fct table")
#     #
#     # client = storage.Client()
#     # bucket = client.get_bucket(bucket)
#     # blob = bucket.blob(name)
#     # _ = bucket.copy_blob(blob, DESTINATION_BUCKET, name)
#     # blob.delete()
