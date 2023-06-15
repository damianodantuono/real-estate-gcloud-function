import functions_framework
from google.cloud import bigquery
from google.cloud import storage
from pathlib import Path
import datetime
import os
import requests
import time
import pendulum


def notifier():
    def build_message(title, price, link) -> str:
        return f"""Nuovo annuncio trovato!
    \U0001F3E0: {title}
    \U0001F4B0: {price}
    \U0001F517: {link}"""

    def send_tgram_message(message: str, chat_id: int, bot_token_api: str) -> None:
        send_message = requests.post(f"https://api.telegram.org/bot{bot_token_api}/sendMessage",
                                     data={"chat_id": chat_id, "text": message})
        if send_message.status_code != 200:
            raise Exception(f"Error sending message to chat_id: {chat_id}. Status code: {send_message.status_code}")

    start_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"This run starts: {start_datetime}")
    client = bigquery.Client()
    with open('sql_scripts/fetch_new_data.sql', 'r') as f:
        FETCH_NEW_DATA_QUERY = f.read()
    query_job = client.query(FETCH_NEW_DATA_QUERY)
    results = query_job.result()
    if results.total_rows == 0:
        print("No new data found")
    else:
        chat_id = int(os.getenv('CHAT_ID'))
        bot_token_api = os.getenv('BOT_API_TOKEN')
        if results.total_rows == 1:
            message = f"Trovata una nuova inserzione."
        else:
            message = f"Trovate {results.total_rows} nuove inserzioni."
        message += '\nParametri di ricerca: \n- Prezzo: ≤ 1000 EUR/MESE\n- Città: Como'
        send_tgram_message(message, chat_id, bot_token_api)
        i = 0
        for row in results:
            message = build_message(row['Title'], row['price'], row['link'])
            send_tgram_message(message, chat_id, bot_token_api)
            time.sleep(2)
            i += 1
            if i % 5 == 0:
                time.sleep(30)
    end_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"This run ends: {end_datetime}")
    with open('sql_scripts/update_notifier_runner_configuration.sql', 'r') as f:
        UPDATE_NOTIFIER_RUNNER_CONFIGURATION = f.read().format(start_time=start_datetime, end_time=end_datetime)
    _ = client.query(UPDATE_NOTIFIER_RUNNER_CONFIGURATION.format(start_time=start_datetime, end_time=end_datetime))
    return "OK"


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def file_processing(cloud_event):
    data = cloud_event.data
    bucket = data["bucket"]
    name = data["name"]
    filename = Path(name).name

    # fetch event time
    SECONDS_TO_IGNORE = 10
    event_time = pendulum.parse(cloud_event['time'])

    # if event time is older than 1 minute, ignore it
    if (pendulum.now(tz='UTC') - event_time).total_seconds() > SECONDS_TO_IGNORE:
        print(f"Ignoring event older than {SECONDS_TO_IGNORE} seconds")
        return "OK"

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

    notifier()
