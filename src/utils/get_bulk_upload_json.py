"""Transform list of string data into one json string with next line breaks as separator."""

import json


def get_bulk_upload_json(data: list) -> str:
    """
    Transform list of string data into one json string with next line breaks as separator.

    :param data: List of string data.
    :return: Json string with next line breaks as separator.
    """
    json_data = []

    for item in data:
        ind = {"index": {"_id": item["_id"]}}
        json_data.append(json.dumps(ind))
        del item["_id"]
        # The replace method is here because ES only takes them in lower case,
        # sending the first letter in upper case causes a parse error
        json_data.append(
            json.dumps(item, default=str)
            .replace("True", "true")
            .replace("False", "false")
        )

    json_data = "\n".join(json_data)
    json_data = json_data + "\n"

    return json_data
