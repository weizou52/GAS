# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import sys
import time
import driver
import boto3
import json
import botocore
import shutil
import os
from botocore.exceptions import ClientError, UnknownServiceError



"""A rudimentary timer for coarse-grained profiling
"""
class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")

if __name__ == '__main__':

    user_id = sys.argv[3]
    JOBS_DIR = sys.argv[4]
    region=sys.argv[5]
    iam_username=sys.argv[6]
    AWS_S3_RESULTS_BUCKET=sys.argv[7]
    tb=sys.argv[8]
    stateMachineArn=sys.argv[9]

    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        with Timer():
            driver.run(sys.argv[1], 'vcf')

        # Reference for A7
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
        # https://pynative.com/python-delete-files-and-directories/

        # 1. Upload the results file to S3 results bucket
        try:
            s3_client = boto3.client('s3', region_name=region)
        except UnknownServiceError as error:
            sys.exit(1)
        try:
            file_name = sys.argv[1].split('/')[-1].split('.')[0]
            job_id=sys.argv[2]
        except:
            print("Cannot get file_name and job_id!")
            sys.exit(1)
            
        job_path=f"{JOBS_DIR}/{job_id}/"
        result_path=f"{iam_username}/{sys.argv[3]}/{file_name}"
        annot_file=result_path+".annot.vcf"
        log_file=result_path+".vcf.count.log"
                
        try:
            response = s3_client.upload_file(job_path+file_name+".annot.vcf", AWS_S3_RESULTS_BUCKET, annot_file)
        except ClientError as e:
            print("Cannot upload the results file to S3")
            sys.exit(1)
        
        # 2. Upload the log file to S3 results bucket
        try:
            response = s3_client.upload_file(job_path+file_name+".vcf.count.log", AWS_S3_RESULTS_BUCKET, log_file)
        except ClientError as e:
            print("Cannot upload the log file to S3")
            sys.exit(1)


        # 3. Invoke Stepfunction
        try:
            stepFn=boto3.client('stepfunctions', region_name=region)
            response=stepFn.start_execution(stateMachineArn=stateMachineArn, input=json.dumps({'job_id':job_id, 'user_id':user_id, 'annot_file': annot_file}))
        except ClientError as e:
            print("Cannot upload the log file to S3")
    
        # 4. Updates the job item in the DynamoDB table 
        # https://stackoverflow.com/questions/34447304/example-of-update-item-in-dynamodb-boto3
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.update_item
        try:
            db = boto3.resource('dynamodb', region_name=region)
            table = db.Table(tb)
            table.update_item(
                Key={'job_id':job_id},
                UpdateExpression='SET #att1=:val1, #att2=:val2, #att3=:val3, #att4=:val4, #att5=:val5',
                ExpressionAttributeNames={"#att1": "s3_results_bucket","#att2": "s3_key_result_file",
                    "#att3": "s3_key_log_file","#att4": "complete_time","#att5": "job_status"},
                ExpressionAttributeValues={":val1": "gas-results",":val2": annot_file,":val3": log_file,
                ":val4": round(time.time()),":val5": "COMPLETED"},
                ReturnValues="UPDATED_NEW",
            )
        except ClientError as e:
            print("Cannot insert data to the table")
            sys.exit(1)


            

        # 5. Clean up (delete) local job files
        shutil.rmtree(job_path)

    else:
        print("A valid .vcf file must be provided as input to this program.")

### EOF