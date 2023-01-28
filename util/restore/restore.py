# restore.py
#
# Restores thawed data, saving objects to S3 results bucket
# NOTE: This code is for an AWS Lambda function
#
# Copyright (C) 2011-2021 Vas Vasiliadis
# University of Chicago
##

import boto3
import time
import os
import sys
import json
from botocore.exceptions import ClientError

# Define constants here; no config file is used for Lambdas
DYNAMODB_TABLE = "weizou_annotations"
VAULT_NAME = "ucmpcs"
REGION_NAME = "us-east-1"
RESULT_BUCKET = "gas-results"

# https://docs.aws.amazon.com/lambda/latest/dg/python-handler.html
def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    message = json.loads(event["Records"][0]["Sns"]["Message"])
    print("Received message", message)
    glacier_jobid = message["JobId"]
    print("Glacier job id is", glacier_jobid)

    # Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#archive
    print("Getting thawed glacier data")
    try:
        glacier = boto3.resource('glacier', region_name=REGION_NAME)
        job = glacier.Job(account_id='-', vault_name=VAULT_NAME, id=glacier_jobid)
        data = job.get_output()
        data = data['body'].read()
    except ClientError as e:
        print(e)
        raise e

    print("Extracting job id from SNS topic")
    archive_id = job.archive_id
    metadata = json.loads(job.job_description)
    try:
        job_id = metadata['job_id']
    except KeyError as e:
        print(e)
        raise e

    print("Extracting user_id and s3_key_result_file from dynamodb")
    try:
        dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
        ann_table = dynamodb.Table(DYNAMODB_TABLE)
        response = ann_table.get_item(Key = {'job_id': job_id})
    except ClientError as e:
        raise e
    try:
        item = response['Item']
        user_id = item['user_id']
        s3_key_result_file = item['s3_key_result_file']
    except KeyError as e:
        raise e

    print("Copying restored object back to S3 results bucket")
    try:
        s3 = boto3.client('s3', region_name=REGION_NAME)
        response = s3.put_object(
                            Body = data,
                            Bucket = RESULT_BUCKET,
                            Key = s3_key_result_file)
    except ClientError as e:
        print(e)
        raise e

    print("Deleting Glacier archive")
    try:
        glacier.Archive(account_id='-', vault_name=VAULT_NAME, id=archive_id).delete()
    except ClientError as e:
        print(e)
        raise e

    print("Updating glacier job status in dynamodb")    
    try:
        response = ann_table.update_item(
            Key={'job_id': job_id},
            UpdateExpression="SET glacier_job_status = :val1",
            ExpressionAttributeValues={':val1': 'RESTORED'})
    except ClientError as e:
        print("Cannot update items to DynamoDB: ", e)

    print("Lambda function successfully completed")

### EOF