# aws-s3-to-opensearch-pipeline

This package deploys a test AWS OpenSearch cluster along with a load mechanism to index daily batches of big data
in a fast and scalable manner.

# Requirements

* Python 3.10
* NPM 2.21 or higher
* CDK CLI
* AWS CLI
* An AWS account

# What will this stack create?

* An AWS OpenSearch cluster
* A Lambda function to index data
* An SQS queue
* A Glue Job

# Deployment and setup

* Set up the AWS CLI to use credentials with admin permissions in your account:

```shell
aws configure
```

* Clone the package and move into that directory:

```shell
git clone ....
cd aws-s3-to-opensearch-pipeline
```

* Create a `venv`:

```shell
python3.10 -m venv ./venv && source venv/bin/activate
```

* Modify the following parameters in `lib/configs.ts`:
    * `glueTableName`: Name of the Glue table to index.
    * `glueSchemaName`: Name of the Glue schema where the table is.
    * `glueTableAccountId`: Account ID where the table is located.
    * `dateFilterColumnName`: Name of the date column in the table to use as a date filter.

* Run the following command:

```shell
npm install && npm run build && cdk bootstrap && cdk deploy
```

* Once the deployment is successful, you will have a new IAM role, ensure it has permissions to the glue table in
  `lib/configs.ts`, the role's ARN looks like this:
    * `arn:aws:iam::<your-account-id>:role/OpenSearchGlueRole-<your-region>`

* The glue job is scheduled to run every day, you can log in to the Kibana dashboards using the newly created secret
  in AWS Secrets Manager and the URL in the AWS OpenSearch console.

# Statistics

A production version of this stack is used in AWS Hardware Engineering to monitor System Event Logs (SEL) and help
minimize hardware errors for customers. Here are some numbers for the data sets used for benchmarking this stack:

| Data Set   | Daily Avg Rows | Daily Size in S3 in Parquet | Daily Size in Opensearch After indexing |
|------------|----------------|-----------------------------|-----------------------------------------|
| `DataSetA` | ~880,000,000   | 90 GB                       | ~260 GB                                 |
| `DataSetB` | ~250,000,000   | 2.5 GB                      | ~25 GB                                  |

And here are some benchmarks for indexing this data:

| Data Set   | Instance Type  | Data Nodes | EBS Size (GiB) | Time to index (avg) |
|------------|----------------|------------|----------------|---------------------|
| `DataSetA` | `r6g.12xlarge` | 4          | 24576          | 44m 15s             |
| `DataSetA` | `r6g.12xlarge` | 10         | 24576          | 19m 20s             |
| `DataSetA` | `r6g.12xlarge` | 20         | 24576          | 11m 44s             |
| `DataSetB` | `r6g.12xlarge` | 4          | 24576          | 11m 59s             |
| `DataSetB` | `r6g.12xlarge` | 10         | 24576          | 7m 27s              |
| `DataSetB` | `r6g.12xlarge` | 20         | 24576          | 5m 8s               |

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.