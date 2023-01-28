# archive_script.py
#
# Archive free user data
#
# Copyright (C) 2011-2021 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import boto3
import time
import os
import sys
import json
import psycopg2
from botocore.exceptions import ClientError

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('archive_script_config.ini')

region=config['aws']['AwsRegionName']
RESULT_BUCKET=config['s3']['AWS_S3_RESULTS_BUCKET']
VAULT_NAME=config['glacier']['VAULT_NAME']
TABLE_NAME=config['dynamodb']['AWS_DYNAMODB_ANNOTATIONS_TABLE']


'''Capstone - Exercise 7
Archive free user results files
'''
def handle_archive_queue(sqs=None):
    
  # Read a message from the queue
  try:
    response = sqs.receive_message(
      QueueUrl=config['sqs']['QueueUrl'],
      MaxNumberOfMessages=int(config['sqs']['MaxNumberOfMessages']),
      WaitTimeSeconds=int(config['sqs']['WaitTimeSeconds'])
    )  
  except ClientError as e:
    print(f'Cannot read a message from the queue!: {e}')
    sys.exit(1)

  # Process message
  if len(response.get('Messages', []))>0:
    print('Processing the message')
    for message in response.get("Messages", []):
      try:
        message_body = message["Body"]
        data = json.loads(json.loads(message_body)['Message'])
        # print(data)
        job_id=data['job_id']
        user_id=data['user_id']
        annot_file=data['annot_file']
        print(f'Processing job: {job_id}')
      except KeyError as error:
        print(f'Cannot parse extract job_id from archival message')
        # Delete message  
        print('Deleting the archival message')
        try:
          response = sqs.delete_message(
            QueueUrl=config['sqs']['QueueUrl'],
            ReceiptHandle=message['ReceiptHandle']
          )

        except ClientError as e:
          print(f'Cannot delete message!: {e}')
        continue


      # Check the user role
      print('Checking the user role')
      try:
        user_profile = helpers.get_user_profile(id = user_id)
        user_role = user_profile[4]
      except ClientError as e:
        print('Cannot get the user role!')
        sys.exit(1)

      if user_role == config['gas']['PREMIUM_USER_IDENTIFIER']:
        # Premium user, nothing to be done
        print('Premium user:)')
        print('Deleting the archival message')
        try:
          response = sqs.delete_message(
            QueueUrl=config['sqs']['QueueUrl'],
            ReceiptHandle=message['ReceiptHandle']
          )

        except ClientError as e:
          print(f'Cannot delete message!: {e}')
        return

      assert user_role == config['gas']['FREE_USER_IDENTIFIER']
      print("Still a free user, archiving")
      try:
        s3 = boto3.client('s3', region_name=region)
        data = s3.get_object(Bucket=RESULT_BUCKET, Key=annot_file)
        archive_file = data['Body'].read()
      except ClientError as e:
        print('Cannot read result file from s3')
        # Delete message  
        print('Deleting the archival message')
        try:
          response = sqs.delete_message(
            QueueUrl=config['sqs']['QueueUrl'],
            ReceiptHandle=message['ReceiptHandle']
          )

        except ClientError as e:
          print(f'Cannot delete message!: {e}')
        continue


      print("Archiving to Glacier")
      # https://docs.aws.amazon.com/code-samples/latest/catalog/python-glacier-upload_archive.py.html
      try:
        glacier = boto3.client('glacier', region_name = region)
        archive = glacier.upload_archive(vaultName=VAULT_NAME, body=archive_file)
      except ClientError as e:
        sys.exit(1)
      print("Deleting data from S3")
      try:
        s3.delete_object(Bucket=RESULT_BUCKET, Key=annot_file)
      except ClientError as e:
        sys.exit(1)

      print("Putting archive ID to DynamoDB")
      try:
        dynamodb = boto3.resource('dynamodb', region_name=region)
        ann_table = dynamodb.Table(TABLE_NAME)
        response = ann_table.update_item(
          Key={'job_id': job_id},
          UpdateExpression="SET results_file_archive_id = :archive_id",
          ExpressionAttributeValues={':archive_id': archive['archiveId']})
      except ClientError as e:
        print("Cannot put archive ID to DynamoDB ", e)
        sys.exit(1)

      # Delete message  
      print('Deleting the archival message')
      try:
        response = sqs.delete_message(
          QueueUrl=config['sqs']['QueueUrl'],
          ReceiptHandle=message['ReceiptHandle']
        )
      except ClientError as e:
        print(f'Cannot delete message!: {e}')

   

def main():
  # Get handles to resources; and create resources if they don't exist
  try:
    sqs=boto3.client("sqs",region_name=region)
  except UnknownServiceError as e:
    print(f'No sqs service!: {e}')
    sys.exit(1)
  # Poll queue for new results and process them
  while True:
    handle_archive_queue(sqs=sqs)

if __name__ == '__main__':  
  main()

### EOF