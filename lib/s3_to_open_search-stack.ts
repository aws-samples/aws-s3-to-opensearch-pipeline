import * as cdk from 'aws-cdk-lib';
import {Duration, RemovalPolicy, Stack} from 'aws-cdk-lib';
import {Construct} from 'constructs';
import {EbsDeviceVolumeType} from 'aws-cdk-lib/aws-ec2';
import {Domain, EngineVersion, TLSSecurityPolicy} from 'aws-cdk-lib/aws-opensearchservice';
import {Key} from 'aws-cdk-lib/aws-kms';
import {Secret} from 'aws-cdk-lib/aws-secretsmanager';
import {
    AnyPrincipal, Effect, ManagedPolicy, PolicyStatement, Role, ServicePrincipal,
} from 'aws-cdk-lib/aws-iam';
import {
    Code as GlueCode, GlueVersion, Job, JobExecutable, PythonVersion, WorkerType,
} from '@aws-cdk/aws-glue-alpha';
import {CfnTrigger} from 'aws-cdk-lib/aws-glue';
import * as path from 'path';
import {
    BlockPublicAccess, Bucket, BucketEncryption, EventType,
} from 'aws-cdk-lib/aws-s3';
import {Queue, QueueEncryption} from 'aws-cdk-lib/aws-sqs';
import {SqsDestination} from 'aws-cdk-lib/aws-s3-notifications';
import {
    Code, Function, LayerVersion, Runtime,
} from 'aws-cdk-lib/aws-lambda';
import {SqsEventSource} from 'aws-cdk-lib/aws-lambda-event-sources';
import {Asset} from 'aws-cdk-lib/aws-s3-assets';
import {Rule, Schedule} from 'aws-cdk-lib/aws-events';
import {LambdaFunction} from 'aws-cdk-lib/aws-events-targets';
import configs from './configs';

export default class S3ToOpenSearchStack extends cdk.Stack {
    // vpc: Vpc;

    domain: Domain;

    constructor(scope: Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, props);

        if (configs.glueTableName === 'placeholder' || configs.glueSchemaName === 'placeholder' || configs.glueTableAccountId === 'placeholder') {
            throw new Error('glueTableName, glueSchemaName and glueTableAccountId must be set in lib/configs');
        }

        /// ////////////////////////////////////////
        /// ////////////////////////////////////////
        /// ////////////////////////////////////////
        /// ////////////////////////////////////////
        // Roles

        const lambdaRole = new Role(this, 'OpenSearchLambdaLoadRole', {
            roleName: `OpenSearchLambdaLoadRole-${Stack.of(this).region}`,
            assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
            managedPolicies: [
                ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'),
                ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
            ],
        });

        const glueRole = new Role(this, 'OpenSearchGlueRole', {
            roleName: `OpenSearchGlueRole-${Stack.of(this).region}`,
            assumedBy: new ServicePrincipal('glue.amazonaws.com'),
            managedPolicies: [
                ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
            ],
        });

        glueRole.addToPolicy(new PolicyStatement({
            effect: Effect.ALLOW,
            actions: [
                'lakeformation:GetDataAccess',
                's3:Get*',
                'kms:Decrypt',
                'glue:GetTable',
            ],
            resources: [
                '*',
            ],
        }));

        /// ////////////////////////////////////////
        /// ////////////////////////////////////////
        /// ////////////////////////////////////////
        /// ////////////////////////////////////////
        // OS Domain

        const ebsVolumeSize = configs.osDriveSizeGib;
        const instanceType = configs.osInstanceTypes;
        const instanceTypeVcpus = configs.osInstanceTypeVcpus;
        const masterNodes = 3;
        const dataNodes = configs.osDataNodeCount;
        const opensearchUsername = 'osadmin';

        const openSearchSecretKmsKey = new Key(this, 'OpenSearchSecretKMSKey', {
            enableKeyRotation: true,
            removalPolicy: RemovalPolicy.DESTROY,
        });

        const openSearchSecret = new Secret(this, 'OpenSearchSecret', {
            encryptionKey: openSearchSecretKmsKey,
            secretName: 'OpenSearchSecret',
            generateSecretString: {
                secretStringTemplate: JSON.stringify({
                    username: opensearchUsername,
                }),
                generateStringKey: 'password',
                excludeCharacters: '/@" ;%$!\'',
            },
        });

        const openSearchKmsKey = new Key(this, 'OpenSearchKMSKey', {
            enableKeyRotation: true,
            removalPolicy: RemovalPolicy.DESTROY,
        });

        this.domain = new Domain(this, 'OpenSearchDomain2', {
            version: EngineVersion.OPENSEARCH_2_7,
            domainName: 'myosdomain2',
            ebs: {
                volumeSize: ebsVolumeSize,
                volumeType: EbsDeviceVolumeType.GP3,
                iops: ebsVolumeSize <= 1024 ? 3000 : Math.min(50000, ebsVolumeSize * 3),
            },
            enableVersionUpgrade: true,
            encryptionAtRest: {
                enabled: true,
                kmsKey: openSearchKmsKey,
            },
            enforceHttps: true,
            zoneAwareness: {
                availabilityZoneCount: 2,
            },
            capacity: {
                masterNodes,
                masterNodeInstanceType: instanceType,
                dataNodes,
                dataNodeInstanceType: instanceType,
                multiAzWithStandbyEnabled: false,
            },
            logging: {
                slowSearchLogEnabled: true,
                appLogEnabled: true,
                slowIndexLogEnabled: true,
                auditLogEnabled: true,
            },
            fineGrainedAccessControl: {
                masterUserName: opensearchUsername,
                masterUserPassword: openSearchSecret.secretValueFromJson('password'),
            },
            nodeToNodeEncryption: true,
            tlsSecurityPolicy: TLSSecurityPolicy.TLS_1_2,
            removalPolicy: RemovalPolicy.DESTROY,
            accessPolicies: [
                new PolicyStatement({
                    actions: [
                        '*',
                    ],
                    effect: Effect.ALLOW,
                    principals: [
                        new AnyPrincipal(),
                    ],
                }),
            ],
        });

        openSearchSecret.grantRead(lambdaRole);
        openSearchSecret.grantRead(glueRole);

        /// ////////////////////////////////////////
        /// ////////////////////////////////////////
        /// ////////////////////////////////////////
        /// ////////////////////////////////////////
        // SQS

        const sqsKmsKey = new Key(this, 'OpenSearchSQSLoadQueueKMSKey', {
            enableKeyRotation: true,
            removalPolicy: RemovalPolicy.DESTROY,
        });

        const loadQueue = new Queue(this, 'OpenSearchSQSLoadQueue', {
            encryption: QueueEncryption.KMS,
            encryptionMasterKey: sqsKmsKey,
            queueName: 'OpenSearchSQSLoadQueue',
            visibilityTimeout: Duration.minutes(5),
        });

        /// ////////////////////////////////////////
        /// ////////////////////////////////////////
        /// ////////////////////////////////////////
        /// ////////////////////////////////////////
        // S3 Bucket

        const uploadsPrefix = 'os-uploads';

        const s3KmsKey = new Key(this, 'S3BucketKMSKey', {
            enableKeyRotation: true,
            removalPolicy: RemovalPolicy.DESTROY,
        });

        const s3Bucket = new Bucket(this, 'S3Bucket', {
            bucketName: `open-search-load-bucket-${Stack.of(this).region}-${Stack.of(this).account}`,
            encryptionKey: s3KmsKey,
            enforceSSL: true,
            blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
            bucketKeyEnabled: true,
            encryption: BucketEncryption.KMS,
        });

        s3Bucket.grantRead(lambdaRole);
        s3Bucket.grantReadWrite(glueRole);

        s3Bucket.addEventNotification(
            EventType.OBJECT_CREATED,
            new SqsDestination(loadQueue),
            {
                prefix: uploadsPrefix,
                suffix: '.json',
            }
        );

        s3Bucket.addLifecycleRule({
            expiration: Duration.days(20),
            id: 'LoadBucketExpiration',
            prefix: uploadsPrefix,
        });

        /// ////////////////////////////////////////
        /// ////////////////////////////////////////
        /// ////////////////////////////////////////
        /// ////////////////////////////////////////
        // Glue Job

        const glueAssets = new Asset(this, 'OpenSearchGlueAssets', {
            path: path.join(__dirname, '../src/'),
        });

        const defaultArguments = {
            '--hostname': this.domain.domainEndpoint,
            '--uploadsPrefix': uploadsPrefix,
            '--tableName': configs.glueTableName,
            '--schemaName': configs.glueSchemaName,
            '--uploadBucketName': s3Bucket.bucketName,
            '--dateFilterColumnName': configs.dateFilterColumnName,
            '--openSearchSecretName': openSearchSecret.secretName,
            '--accountId': configs.glueTableAccountId,
            '--region': Stack.of(this).region,
            '--additional-python-modules': 'boto3,pyspark,opensearch-py',
            '--extra-py-files': `s3://${glueAssets.s3BucketName}/${glueAssets.s3ObjectKey}`,
        };

        const glueJob = new Job(this, 'OpenSearchGlueJob', {
            executable: JobExecutable.pythonEtl({
                glueVersion: GlueVersion.V4_0,
                pythonVersion: PythonVersion.THREE,
                script: GlueCode.fromAsset(path.join(__dirname, '../src/glue_job.py')),
            }),
            role: glueRole,
            jobName: 'OpenSearchLoadJob',
            defaultArguments,
            workerType: WorkerType.G_2X,
            workerCount: 20,
            enableProfilingMetrics: true,
            timeout: Duration.minutes(60),
            maxRetries: 0,
        });

        new CfnTrigger(this, 'OpenSearchGlueJobTrigger', {
            type: 'SCHEDULED',
            schedule: 'cron(0 2 * * ? *)',
            actions: [
                {
                    jobName: glueJob.jobName,
                    arguments: defaultArguments,
                },

            ],
            startOnCreation: true,
        });

        /// ////////////////////////////////////////
        /// ////////////////////////////////////////
        /// ////////////////////////////////////////
        /// ////////////////////////////////////////
        // Lambda

        const openSearchPyLayer = new LayerVersion(this, 'OpenSearchPyLayer', {
            code: Code.fromAsset(path.join(__dirname, '../src/libs.zip')),
        });

        const lambdaFunction = new Function(this, 'OpenSearchLoadLambda', {
            functionName: 'OpenSearchLoadLambda',
            code: Code.fromAsset(path.join(__dirname, '../src/')),
            handler: 'lambda_function.os_load_from_s3_json',
            memorySize: 2048,
            timeout: Duration.minutes(5),
            runtime: Runtime.PYTHON_3_10,
            role: lambdaRole,
            environment: {
                hostname: this.domain.domainEndpoint,
                open_search_secret_name: openSearchSecret.secretName,
            },
            retryAttempts: 2,
            reservedConcurrentExecutions: instanceTypeVcpus * dataNodes,
            layers: [
                openSearchPyLayer,
            ],
        });

        const triggerConcurrency = Math.floor(instanceTypeVcpus * dataNodes * 0.5);

        let correctedTriggerConcurrency: number;
        if (triggerConcurrency < 2) {
            correctedTriggerConcurrency = 2;
        } else if (triggerConcurrency > 1000) {
            correctedTriggerConcurrency = 1000;
        } else {
            correctedTriggerConcurrency = triggerConcurrency;
        }

        lambdaFunction.addEventSource(new SqsEventSource(loadQueue, {
            batchSize: 1,
            maxConcurrency: correctedTriggerConcurrency,
        }));

        loadQueue.grantConsumeMessages(lambdaRole);

        /// ////////////////////////////////////////
        /// ////////////////////////////////////////
        /// ////////////////////////////////////////
        /// ////////////////////////////////////////
        // OpenSearch Glue Permissions

        const permissionsFunction = new Function(this, 'OpenSearchGluePermissionsLambda', {
            functionName: 'OpenSearchGluePermissionsLambda',
            code: Code.fromAsset(path.join(__dirname, '../src/')),
            handler: 'permissions_and_settings.permissions_and_settings',
            memorySize: 128,
            timeout: Duration.minutes(1),
            runtime: Runtime.PYTHON_3_10,
            role: lambdaRole,
            environment: {
                hostname: this.domain.domainEndpoint,
                open_search_secret_name: openSearchSecret.secretName,
                glue_role_arn: glueRole.roleArn,
                lambda_role_arn: lambdaRole.roleArn,
            },
            retryAttempts: 2,
        });

        const eventRule = new Rule(this, 'ScheduleRule', {
            schedule: Schedule.cron({
                minute: '*',
            }),
        });

        eventRule.addTarget(new LambdaFunction(permissionsFunction));
    }
}
