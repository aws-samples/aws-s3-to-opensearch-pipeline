"""Glue job to dump a Glue catalog table to S3 as JSON."""

import sys
import time
from datetime import datetime, timedelta

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from opensearchpy import OpenSearch
from opensearchpy.client.cat import CatClient
from opensearchpy.client.indices import IndicesClient
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import monotonically_increasing_id

from utils.generate_os_table_definition import generate_os_table_definition
from utils.get_secret import get_secret

args = getResolvedOptions(
    sys.argv,
    [
        "hostname",
        "uploadsPrefix",
        "tableName",
        "schemaName",
        "uploadBucketName",
        "dateFilterColumnName",
        "openSearchSecretName",
        "accountId",
        "region",
    ],
)

hostname: str = args["hostname"]
uploads_prefix: str = args["uploadsPrefix"]
table_name: str = args["tableName"]
schema_name: str = args["schemaName"]
upload_bucket_name: str = args["uploadBucketName"]
date_filter_column_name: str = args["dateFilterColumnName"]
open_search_secret_name: str = args["openSearchSecretName"]
account_id: str = args["accountId"]
region: str = args["region"]

yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
index_name = f"{schema_name}_{table_name}_{yesterday}"
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

indices_client = IndicesClient(os_client)
cat_client = CatClient(os_client)
data_node_count = len(
    [i for i in cat_client.nodes(format="json") if "data" in i["node.roles"]]
)

conf = SparkConf()
conf.set("spark.driver.maxResultSize", "10g")
spark_context = SparkContext(conf=conf)
glue_context = GlueContext(spark_context)

# Check if index exists, delete if it does
if indices_client.exists(index_name):
    indices_client.delete(index_name)

# Get mappings from glue table

mappings = generate_os_table_definition(schema_name, table_name, region, account_id)

# Create index

indices_client.create(
    index_name,
    body={
        "settings": {
            "index": {
                "number_of_shards": data_node_count,
                "number_of_replicas": 1,
                "codec": "best_compression",
            }
        },
        "mappings": {
            "properties": mappings,
        },
    },
)

# Load data to DF

df_kwargs = {
    "database": schema_name,
    "table_name": table_name,
    "push_down_predicate": f"{date_filter_column_name}=='{yesterday}'",
    "catalog_id": account_id,
}

data = glue_context.create_dynamic_frame_from_catalog(**df_kwargs).toDF()

# Drop to S3

data = data.withColumn("_id", monotonically_increasing_id())
output_path = f"s3://{upload_bucket_name}/{uploads_prefix}/{schema_name}/{table_name}/{index_name}"

data.write.mode("overwrite").option("maxRecordsPerFile", 25000).format("json").save(
    output_path
)

# Wait for indexing

original_row_count = data.count()

while original_row_count > int(
    cat_client.count(index=index_name, format="json")[0]["count"]
):
    time.sleep(15)
