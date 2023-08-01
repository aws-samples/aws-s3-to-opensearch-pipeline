"""Lambda function to load data from JSON line files in S3 to an OS cluster."""
import json
import logging
import os
from typing import Dict, Union

import boto3
from opensearchpy.client import OpenSearch

from utils.calculate_bulk_index_size import calculate_bulk_index_size
from utils.get_bulk_upload_json import get_bulk_upload_json
from utils.get_secret import get_secret


def os_load_from_s3_json(event: dict, _: dict) -> Dict[str, Union[str, int]]:
    """
    Receive s SQS message, read the S3 file in the message and load it to OS.

    :param event: Event received in Lambda from SQS.
    :param _: Unused context provided by Lambda.
    :return: None
    """
    hostname = os.environ["hostname"]
    open_search_secret_name = os.environ["open_search_secret_name"]
    filter_path = "-took,-items.index._*,-items.index.result"

    secret = get_secret(open_search_secret_name)

    os_client = OpenSearch(
        hosts=[
            {
                "host": hostname,
                "port": 443,
            },
        ],
        http_compress=True,
        http_auth=(secret["username"], secret["password"]),
        use_ssl=True,
    )

    for r in event["Records"]:
        records = json.loads(r["body"])
        for m in records["Records"]:
            bucket = m["s3"]["bucket"]["name"]
            key = m["s3"]["object"]["key"]
            table = key.split("/")[-2]
            file_name = f"s3://{bucket}/{key}"
            try:
                s3obj = boto3.resource("s3").Object(bucket_name=bucket, key=key)
                file_content = s3obj.get()["Body"].read().decode("utf-8")
                all_rows = file_content.splitlines()

                sample_size = 100
                batch_size = (
                    calculate_bulk_index_size(
                        all_rows[:sample_size], max_http_payload_size_bytes=52428800
                    )
                    if len(all_rows) >= sample_size
                    else sample_size
                )

                logging.info(f"Optimal batch_size: {batch_size}.")

                json_content = []
                counter = 0
                for row in all_rows:
                    counter = counter + 1
                    json_content.append(json.loads(row))
                    if counter >= batch_size:
                        os_client.bulk(
                            body=get_bulk_upload_json(json_content),
                            index=table,
                            filter_path=filter_path,
                        )
                        counter = 0
                        json_content = []

                if json_content:
                    os_client.bulk(
                        body=get_bulk_upload_json(json_content),
                        index=table,
                        filter_path=filter_path,
                    )

            except Exception as e:
                print(f"Error processing {file_name}")
                raise e

    return {"StatusCode": 200, "Message": "SUCCESS"}
