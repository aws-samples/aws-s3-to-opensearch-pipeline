"""Generate the Table Column Definition required for Opensearch Index Creation."""

from utils.get_table_columns_metadata import get_table_columns_metadata


def generate_os_table_definition(
    schema_name: str, table_name: str, region: str, account_id: str
) -> dict:
    """
    Generate the Table Column Definition required for Opensearch Index Creation.

    :param schema_name: Name of Glue database.
    :param table_name: Name of Glue Table.
    :param region: AWS region the table is on.
    :param account_id: Account where the table is located.
    :return: Dictionary with columns and column types.
    """
    columns_metadata = get_table_columns_metadata(
        schema_name, table_name, region, account_id
    )
    os_column_mapping_definition = {}
    type_key = "Type"
    name_key = "Name"

    for column in columns_metadata:
        if column[type_key] in (
            "date",
            "timestamp with time zone",
            "timestamp",
        ) or column[name_key] in ("run_date", "snapshot_day", "snapshot_date"):
            data_type = "date"
        elif column[type_key] in ("char", "varchar", "array", "map", "string"):
            data_type = "keyword"
        elif column[type_key] in ("int", "tinyint", "smallint", "integer"):
            data_type = "integer"
        elif column[type_key] in ("bigint",):
            data_type = "long"
        elif (
            column[type_key] in ("double", "decimal", "real", "float")
            or "decimal" in column[type_key]
        ):
            data_type = "double"
        elif column[type_key] == "boolean":
            data_type = "boolean"
        else:
            raise Exception("Data Type " + str(column[type_key]) + " unknown.")

        os_column_mapping_definition[str(column[name_key])] = {"type": data_type}

        if data_type == "date":
            os_column_mapping_definition[str(column[name_key])][
                "format"
            ] = "yyyy-MM-dd HH:mm:ss.SSSSSS||yyyy-MM-dd HH:mm:ss||strict_date_optional_time||epoch_millis"

    return os_column_mapping_definition
