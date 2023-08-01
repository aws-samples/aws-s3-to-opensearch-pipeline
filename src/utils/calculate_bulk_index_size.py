"""Calculate bulk_index_size depending on the average content size."""

import json


def calculate_bulk_index_size(
    json_content: list, max_http_payload_size_bytes: int = 10485760
) -> int:
    """
    Calculate bulk_index_size depending on the average content size.

    Formula: (total size in bytes of all rows / number of rows) + constant overhead from new lines and document id.

    :param max_http_payload_size_bytes: Max allowed http payload size.
    Depends on the instance type and network quota quoted here:
    https://docs.aws.amazon.com/opensearch-service/latest/developerguide/limits.html
    :param json_content: list of rows to be indexed.
    :return: return number of rows to be indexed.
    """
    overhead_sample = '{"index": {"_id": 12345678-1234-1234-1234-123456789012}}\n\n'

    average_size = sum(
        [len(json.dumps(row).encode("utf-8")) for row in json_content]
    ) / len(json_content)
    average_size += len(overhead_sample.encode("utf-8"))

    batch_size = max_http_payload_size_bytes * 0.8 / average_size

    return round(batch_size)
