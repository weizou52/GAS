import boto3
from botocore.exceptions import ClientError, UnknownServiceError
import json
import uuid
import subprocess
import os
import sys
from os.path import exists
from boto3.dynamodb.conditions import Key, Attr

from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('ann_config.ini')

BASE_DIR = config['ann']['BASE_DIR']
JOBS_DIR = config['ann']['JOBS_DIR']
RUN_FILE = config['ann']['RUN_FILE']
region=config['aws']['AwsRegionName']
iam_username=config['aws']['iam_username']
AWS_S3_RESULTS_BUCKET=config['s3']['AWS_S3_RESULTS_BUCKET']
tb=config['dynamodb']['AWS_DYNAMODB_ANNOTATIONS_TABLE']
stateMachineArn=config['statemachine']['STATE_MACHINE_ARN']

# reference for A9
# https://hevodata.com/learn/python-sqs/#:~:text=To%20receive%20a%20message%20from,from%20your%20specified%20SQS%20Queue.
# https://docs.aws.amazon.com/code-library/latest/ug/python_3_sqs_code_examples.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Message.delete

# Connect to SQS and get the message queue
try:
    sqs=boto3.client("sqs",region_name=region)
except UnknownServiceError as e:
    print(f'No sqs service!: {e}')
    sys.exit(1)

# Poll the message queue in a loop 
while True:
    # Attempt to read a message from the queue
    # Use long polling - DO NOT use sleep() to wait between polls
    try:
        response = sqs.receive_message(
            QueueUrl=config['sqs']['QueueUrl'],
            MaxNumberOfMessages=int(config['sqs']['MaxNumberOfMessages']),
            WaitTimeSeconds=int(config['sqs']['WaitTimeSeconds']),
        )  
    except ClientError as e:
        print(f'Cannot read a message from the queue!: {e}')
        sys.exit(1)
        

    # If message read, extract job parameters from the message body as before
    if len(response.get('Messages', []))>0:
        for message in response.get("Messages", []):
            try:
                message_body = message["Body"]
            except KeyError as e:
                print(f'Cannot get message body!: {e}')
                sys.exit(1)
                

            # Include below the same code you used in prior homework
            # Get the input file S3 object and copy it to a local file
            # Use a local directory structure that makes it easy to organize
            # multiple running annotation jobs
            try:
                data = json.loads(message_body)['Message']
                data=json.loads(data)
                # print(message)
                # print(data)
                job_id=data['job_id'] # b64b80f0-3716-441e-a717-66a70033a098
                user_id=data['user_id'] # usrX
                input_file_name=data['input_file_name'] # test.vcf
                s3_inputs_bucket=data['s3_inputs_bucket'] # gas-inputs
                s3_key_input_file=data['s3_key_input_file'] # instructor/userX/b64b80f0-3716-441e-a717-66a70033a098~test.vcf
                file_name = s3_key_input_file.split('/')[2] # b64b80f0-3716-441e-a717-66a70033a098~test.vcf
            except KeyError as e:
                print(f'Cannot parse extract job parameters from the message body!: {e}')
                sys.exit(1)
                
                
            try:
                s3_client = boto3.client('s3', region_name=region)
            except UnknownServiceError as e:
                print(f'No aws service!: {e}')
                sys.exit(1)

            # Create the job directory 
            if not exists(JOBS_DIR):
                os.makedirs(JOBS_DIR)
            try:
                os.makedirs(f'{JOBS_DIR}/{job_id}')
            except FileExistsError as e:
                pass
                

            job_input_file = f'{JOBS_DIR}/{job_id}/{file_name}'
            
            try:
                s3_client.download_file(s3_inputs_bucket, s3_key_input_file, job_input_file)
            except ClientError as e:
                print(f'Cannot download the file!: {e}')
                try:
                    response = sqs.delete_message(
                        QueueUrl=config['sqs']['QueueUrl'],
                        ReceiptHandle=message['ReceiptHandle']
                    )
                except ClientError as e:
                    print(f'Cannot delete message!: {e}')
                    sys.exit(1)
                continue
                

            # Launch annotation job as a background process
            try:
                process = subprocess.Popen(["python", RUN_FILE, job_input_file, f"{job_id}", 
                    f"{user_id}", JOBS_DIR, region, iam_username, AWS_S3_RESULTS_BUCKET,tb, stateMachineArn], cwd = JOBS_DIR + f"/{job_id}")
            except ClientError as e:
                print(f'Cannot launch annotation job!: {e}')
                sys.exit(1)
                
            
            # Update job_status in Dynamodb to Running 
            try:
                db = boto3.resource('dynamodb', region_name=region)
                table = db.Table(config['dynamodb']['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
                # response = table.query(KeyConditionExpression=Key('job_id').eq(job_id))
                # print(response['Items'])
                table.update_item(
                    Key={'job_id':job_id},
                    ConditionExpression='#att=:val1',
                    UpdateExpression='SET #att=:val2',
                    ExpressionAttributeNames={"#att": "job_status"},
                    ExpressionAttributeValues={":val2": "RUNNING",":val1": "PENDING"},
                    ReturnValues="UPDATED_NEW"
                )
            except ClientError as e:
                print(f'Cannot update job status to RUNNING!: {e}')
                
                        

            # Delete the message from the queue, if job was successfully submitted
            try:
                response = sqs.delete_message(
                    QueueUrl=config['sqs']['QueueUrl'],
                    ReceiptHandle=message['ReceiptHandle']
                )
            except ClientError as e:
                print(f'Cannot delete message!: {e}')
                sys.exit(1)
                