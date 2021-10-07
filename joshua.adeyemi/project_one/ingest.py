import openaq
import json
import boto3
import datetime
from dotenv import load_dotenv

load_dotenv()

def ingest_data():
    api = openaq.OpenAQ()

    extracted_data = []
    keep_going = True
    page = 1
    limit=10000
    while keep_going:

        print(f"Page: {page}")

        status, resp = api.measurements(country="NG", limit=limit, page=page)
        if status != 200:
            raise Exception("Error calling this api")

        results = resp["results"]
        if page >= resp["meta"]["pages"]:
            keep_going = False

        extracted_data = extracted_data + results

        page = page + 1
    
    return extracted_data

def store_to_s3(object_list):
    """
    This method will take raw data and store into a location in AWS S3

    What do we need here?
    1. We need a bucket in AWS S3
    2. We need to determine how we will store the data
    """

    jsonl_list = []
    for item in object_list:
        jsonl_list.append(json.dumps(item))
    content = "\n".join(jsonl_list)

    bucket_name = "semicolon-data-lake"

    current_time = datetime.datetime.utcnow()
    object_key = current_time.strftime("year=%Y/month=%m/day=%d/data.jsonl")

    client = boto3.client("s3")
    response = client.put_object(
        Body=bytes(content, "utf-8"),
        Bucket=bucket_name,
        Key=object_key
    )

if __name__ == "__main__":
    extracted_data = ingest_data()
    store_to_s3(extracted_data)