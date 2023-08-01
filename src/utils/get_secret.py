"""Retrieve a secret from AWS Secrets manager based on the secret name."""

import json

import boto3
from botocore.exceptions import ClientError


def get_secret(secret_name: str, region: str = None) -> dict:
    """
    Retrieve a secret from AWS Secrets manager based on the secret name.

    :param secret_name: Name of the secret to retrieve.
    :param region: AWS Region of the service.
    :return: A dictionary with the secret.
    """
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)

    except ClientError as e:
        raise e

    return json.loads(get_secret_value_response["SecretString"])
