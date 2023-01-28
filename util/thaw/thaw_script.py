# thaw_script.py
#
# Thaws upgraded (premium) user data
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
from botocore.exceptions import ClientError


# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('thaw_script_config.ini')

REGION_NAME = config['aws']['AwsRegionName']
TABLE_NAME = config['dynamodb']['table_name']
VAULT_NAME = config['glacier']['vault_name']

'''Capstone - Exercise 9
Initiate thawing of archived objects from Glacier
'''
def handle_thaw_queue(sqs=None):
  # Read a message from the queue
  try:
    messages = sqs.receive_messages(
            MaxNumberOfMessages=int(config['sqs']['MaxNumberOfMessages']),
            WaitTimeSeconds=int(config['sqs']['WaitTimeSeconds']))
  except ClientError as e:
    print(e)
    return
  # Process message

  if len(messages) > 0:
    print(f"Received {str(len(messages))} messages")

    for message in messages:
      try:
        msg_body = json.loads(json.loads(message.body)['Message'])
      except:
        print(f'Cannot get the data from message!')
        try:
          message.delete()
        except ClientError as e:
          print(f"Cannot delete message!")
          return
        continue
      try:
        s3_key_result_file = msg_body['s3_key_result_file']
        job_id = msg_body['job_id']
        user_id = msg_body['user_id']
        archive_id = msg_body['results_file_archive_id']
      except ClientError as e:
        print(f'Cannot parse extract job parameters from the message body!: {e}')
        try:
          message.delete()
        except ClientError as e:
          print(f"Cannot delete message!")
          return
        continue

      # Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.initiate_job
      print(f"Initiating archive retrieval for {job_id}")
      job_params = {"Type": "archive-retrieval", 
                    "ArchiveId": archive_id,
                    "Description": json.dumps({"job_id": job_id}),
                    "SNSTopic": config['sns']['results_restore_topic'],
                    "Tier": "Expedited"}
      try:
        response = glacier.initiate_job(
                        vaultName=VAULT_NAME, 
                        jobParameters=job_params)
      except glacier.exceptions.InsufficientCapacityException as e:
        print("Expedited retrieval didn't work with error:", e)
        print("Attempting standard retrieval")
        try:
          job_params["Tier"] = "Standard"
          response = glacier.initiate_job(
                          vaultName=VAULT_NAME, 
                          jobParameters=job_params)
        except Exception as e:
          print("Exception occured:", e)
          continue

      print("Deleting message")
      # Delete message
      try:
        message.delete()
      except ClientError as e:
        print(f"Cannot delete message: {e} ")

if __name__ == '__main__':  

  # Get handles to resources; and create resources if they don't exist
  sqs_resource=boto3.resource("sqs",region_name=REGION_NAME)
  try:
    sqs = sqs_resource.get_queue_by_name(QueueName=config['sqs']['results_thaw_queue'])
  except ClientError as e:
    print(f'Could not get the thaw queue. Creating the queue...')
    try:
      sqs = sqs_resource.create_queue(QueueName=config['sqs']['results_thaw_queue'],
                                       Attributes={
                                           'MaximumMessageSize': int(config['sqs']['MaxNumberOfMessages']),
                                           'ReceiveMessageWaitTimeSeconds': int(config['sqs']['WaitTimeSeconds'])
                                       })
      print(f'Create the queue!: {e}')
    except ClientError as e:
      print(f'Could not create the thaw queue. {e}')
      sys.exit(1)
  try:
    glacier = boto3.client("glacier", region_name=REGION_NAME)
  except ClientError as e:
    print("Unable to get glacier object", e)
    sys.exit(1)

  # Poll queue for new results and process them
  while True:
    handle_thaw_queue(sqs=sqs)

### EOF