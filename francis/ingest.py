import openaq
import json
from dotenv import load_dotenv
import datetime
import boto3

def ingest_data():
    api = openaq.OpenAQ()
    extracted_data = []
    limit = 10000
    keep_going = True
    page = 1

    while keep_going:
        status, resp = api.measurements(country = "NG", limits = limit, page = page)
        if status != 200:
            raise Exception("error fetching data from api")
        results = resp['results']

        if page >= resp['meta']['pages']:
            keep_going = False
        
        extracted_data = extracted_data + results
        page = page + 1

    return extracted_data

def store_data_s3(objects_list):

    """
    This method will take raw data and store into a location in AWS S3

    What do we need here?
    1. We need a bucket in AWS S3
    2. We need to determine how we will store the data
    """

    jsonl_list = []
    for items in objects_list:
        jsonl_list.append(json.dumps(items))
    contents = "\n".join(jsonl_list)

    bucket_name = "data-eng-practical"
    current_time = datetime.datetime.utcnow()
    object_key = current_time.strftime("year=%y/month=%m/day=%d/data.json")

    client = boto3.client("s3")
    response = client.put_object(
        Body = bytes(contents, "utf8"),
        Bucket = bucket_name,
        Key = object_key
    )

if __name__ == "__main__":
    extracted_data = ingest_data()
    store_data_s3(extracted_data)
