"""Fetch the Glue table column metadata."""
from utils.get_table import get_table


def get_table_columns_metadata(
    schema_name: str, table_name: str, region: str, account_id: str
) -> list[dict[str, str]]:
    """
    Fetch the Glue table column metadata.

    :param schema_name: Name of Glue database.
    :param table_name: Name of Glue Table.
    :param region: AWS region the table is on.
    :param account_id: Account where the table is located.
    :return: List of Dictionaries with column type and name.
    """
    table_metadata = get_table(schema_name, table_name, region, account_id)
    columns_metadata = table_metadata["Table"]["StorageDescriptor"]["Columns"]
    partitions_metadata = (
        table_metadata["Table"]["PartitionKeys"]
        if "PartitionKeys" in table_metadata["Table"]
        else []
    )
    return columns_metadata + partitions_metadata
