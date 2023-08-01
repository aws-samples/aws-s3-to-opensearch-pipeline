"""Get Glue Table information."""

import boto3


def get_table(
    schema_name: str, table_name: str, region: str, account_id: str
) -> dict[str, any]:
    """
    Get Glue Table information.

    :param schema_name: Name of Glue database.
    :param table_name: Name of Glue Table.
    :param region: AWS region the table is on.
    :param account_id: Account where the table is located.
    :return: Glue Table information.
    """
    glue_client = boto3.client("glue", region)
    table_params = {
        "DatabaseName": schema_name,
        "Name": table_name,
        "CatalogId": account_id,
    }

    get_table_response = glue_client.get_table(**table_params)
    return get_table_response
