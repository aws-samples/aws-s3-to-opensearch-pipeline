#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import S3ToOpenSearchStack from '../lib/s3_to_open_search-stack';

const app = new cdk.App();
new S3ToOpenSearchStack(app, 'S3ToOpenSearchStack', {});
