import requests


headers = {
    'Content-Type': 'application/json',
    'ce-id': '123451234512345',
    'ce-specversion': '1.0',
    'ce-time': '2020-01-02T12:34:56.789Z',
    'ce-type': 'google.cloud.storage.object.v1.finalized',
    'ce-source': '//storage.googleapis.com/projects/_/buckets/scraped-data-gh',
    'ce-subject': 'objects/raw_data/como_rent_20230613.parquet.gzip',
}

json_data = {
    'bucket': 'scraped-data-gh',
    'contentType': 'text/plain',
    'kind': 'storage#object',
    'md5Hash': '...',
    'metageneration': '1',
    'name': 'raw_data/como_rent_20230612.parquet.gzip',
    'size': '352',
    'storageClass': 'MULTI_REGIONAL',
    'timeCreated': '2020-04-23T07:38:57.230Z',
    'timeStorageClassUpdated': '2020-04-23T07:38:57.230Z',
    'updated': '2020-04-23T07:38:57.230Z',
}

response = requests.post('http://localhost:8081', headers=headers, json=json_data)
print(response.status_code)
