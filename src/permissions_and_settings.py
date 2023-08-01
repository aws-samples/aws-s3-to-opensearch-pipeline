"""Add the Glue role to the correct OS role."""

import os

from opensearchpy import OpenSearch
from opensearchpy.client.security import SecurityClient

from utils.get_secret import get_secret


def permissions_and_settings(_: any, __: any) -> None:
    """Add the Glue role to the correct OS role."""
    hostname = os.environ["hostname"]
    open_search_secret_name = os.environ["open_search_secret_name"]
    glue_role_arn = os.environ["glue_role_arn"]
    lambda_role_arn = os.environ["lambda_role_arn"]

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

    sec_client = SecurityClient(os_client)
    print(sec_client.get_role("readall_and_monitor"))

    sec_client.patch_role_mapping(
        "all_access",
        [
            {
                "op": "replace",
                "path": "/users",
                "value": [
                    "osadmin",
                    glue_role_arn,
                    lambda_role_arn,
                ],
            }
        ],
    )
