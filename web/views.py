# views.py
#
# Copyright (C) 2011-2022 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime
import boto3
from botocore.client import Config
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template, 
  request, session, url_for)

from app import app, db
from decorators import authenticated, is_premium

"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Open a connection to the S3 service
  s3 = boto3.client('s3', 
    region_name=app.config['AWS_REGION_NAME'], 
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + "/job"

  # Define policy conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f'Unable to generate presigned URL for upload: {e}')
    return abort(500)

  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html',
    s3_post=presigned_post,
    role=session['role'])


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

  region = app.config['AWS_REGION_NAME']

  # Parse redirect URL query parameters for S3 object info
  bucket_name = request.args.get('bucket')
  s3_key = request.args.get('key')

  # Extract the job ID from the S3 key
  try:
    job_id = s3_key.split('/')[2].split('~')[0]
    input_file_name = s3_key.split('/')[2].split('~')[1]
    user_id = s3_key.split('/')[1]
  except ClientError as e:
    app.logger.error(f'Unable to extract the job ID from the S3 key: {e}')
    return abort(500)


  # Persist job to database
  data = { "job_id": job_id, 
            "user_id": user_id, 
            "input_file_name": input_file_name, 
            "s3_inputs_bucket": bucket_name, 
            "s3_key_input_file": s3_key, 
            "submit_time": round(time.time()),
            "job_status": "PENDING"
            }
  
  try:
    dynb = boto3.resource('dynamodb', region_name=region)
    table = dynb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    table.put_item(Item=data)
  except ClientError as e:
    app.logger.error(f'Unable to persist job to database: {e}')
    return abort(500)

  # Send message to request queue
  try:
    sns = boto3.resource('sns', region_name=region)
    topic = sns.Topic(arn=app.config['AWS_SNS_JOB_REQUEST_TOPIC'])
  except:
    app.logger.error(f'Cannot connected to the topic:')
    return abort(500)

  try:
    topic_response = topic.publish(Message=json.dumps(data))
    print('succsess to publish to sns!')
  except:
    app.logger.error(f'Unable to send message to request queue: ')
    return abort(500)


  return render_template('annotate_confirm.html', job_id=job_id)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():

  # Get list of annotations to display
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.query
  region = app.config['AWS_REGION_NAME']
  user_id = session['primary_identity']
  try:
    dynb = boto3.resource('dynamodb', region_name=region)
    table = dynb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    response = table.query(IndexName='user_id_index',
    KeyConditionExpression=Key('user_id').eq(user_id))
  except ClientError as e:
    app.logger.error(f'Unable to persist job to database: {e}')
    return abort(500)
  annotations=response['Items']
  for ann in annotations:
    ann['submit_time']=datetime.fromtimestamp(ann['submit_time'])


  return render_template('annotations.html', annotations=annotations)


"""Display details of a specific annotation job
"""
@app.route('/annotations/<job_id>', methods=['GET'])
@authenticated
def annotation_details(job_id):
  region = app.config['AWS_REGION_NAME']
  user_id = session['primary_identity']
  try:
    dynb = boto3.resource('dynamodb', region_name=region)
    table = dynb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    response = table.get_item(Key = {'job_id': job_id})
  except ClientError as e:
    app.logger.error(f'Unable to persist job to database: {e}')
    return abort(500)
  annotation=response['Item']
  if annotation['user_id']!=user_id:
    return abort(403, description="You are not authorized to check this job!")
  annotation['submit_time']=datetime.fromtimestamp(annotation['submit_time'])

  is_archival = 'download'
  if annotation['job_status'] == "COMPLETED":
    time_passed = time.time()-float(annotation['complete_time'])
    annotation['complete_time']=datetime.fromtimestamp(annotation['complete_time'])

    if (session['role']=='free_user') and ( time_passed > app.config["FREE_USER_DATA_RETENTION"]):
      is_archival = 'upgrade'
    elif session['role'] == "premium_user" \
                and "glacier_job_status" in annotation.keys() \
                and annotation["glacier_job_status"] == "THAWING":
      is_archival = 'restored'

  return render_template('annotation.html', annotation=annotation, is_archival=is_archival)


"""Download the result file contents for an annotation job
"""
# https://stackoverflow.com/questions/60163289/how-do-i-create-a-presigned-url-to-download-a-file-from-an-s3-bucket-using-boto3
# https://stackoverflow.com/questions/26954122/how-can-i-pass-arguments-into-redirecturl-for-of-flask

@app.route('/annotations/<job_id>/result', methods=['GET'])
@authenticated
def annotation_result(job_id):
  s3 = boto3.client('s3', 
    region_name=app.config['AWS_REGION_NAME'], 
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_RESULTS_BUCKET']
  key=request.args.get('s3_key_result_file')
  try:
    presigned_post = s3.generate_presigned_url(
    'get_object',
    Params={'Bucket': bucket_name, 'Key': key},
    ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f'Unable to download the result file : {e}')
    return abort(500)
  if presigned_post is not None:
    return redirect(presigned_post)
  return abort(500)

"""Display the log file contents for an annotation job
"""
# https://stackoverflow.com/questions/31976273/open-s3-object-as-a-string-with-boto3
@app.route('/annotations/<job_id>/log', methods=['GET'])
@authenticated
def annotation_log(job_id):
  s3 = boto3.client('s3', 
    region_name=app.config['AWS_REGION_NAME'])
  bucket_name = app.config['AWS_S3_RESULTS_BUCKET']

  user_id = session['primary_identity']
  key=request.args.get('s3_key_log_file')
  try:
    response=s3.get_object(Bucket=bucket_name,Key=key)
    ob=response['Body'].read().decode('utf-8') 
  except ClientError as e:
    app.logger.error(f'Unable to download the result file : {e}')
    return abort(500)
  return render_template('view_log.html', ob=ob, job_id=job_id)


"""Subscription management handler
"""
import stripe
from auth import update_profile

""" Retrieve data from glacier when user upgrades from free to premium"""
def retrieve_data_from_glacier():
  region=app.config['AWS_REGION_NAME']
  table=app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']

  print("Retrieving data from Glacier")
  try:
    dynamodb = boto3.resource('dynamodb', region_name=region)
    ann_table = dynamodb.Table(table)
    user_id = session.get('primary_identity')
    response = ann_table.query(
      IndexName='user_id_index',
      KeyConditionExpression=Key('user_id').eq(user_id))
  except ClientError as e:
    raise e

  items = response['Items']
  for item in items:
    if "results_file_archive_id" in item.keys() and 'glacier_job_status' not in item.keys():
      data = {'user_id': item['user_id'],
              'job_id': item['job_id'],
              's3_key_result_file': item['s3_key_result_file'],
              'results_file_archive_id': item['results_file_archive_id']}
      print(f"Found new item to be retrieved, archive file with job_id: {item['job_id']}")
      print("Adding glacier job status to dynamodb")    
      try:
        response = ann_table.update_item(
          Key={'job_id': data['job_id']},
          UpdateExpression="SET glacier_job_status = :val1",
          ExpressionAttributeValues={':val1': 'THAWING'})
      except ClientError as e:
        print("Cannot add items to DynamoDB: ", e)
        continue
      print('Publishing the thawing job to sns topic')
      try:
        sns = boto3.client('sns', region_name=region)
        sns.publish(TopicArn=app.config["AWS_SNS_RESULTS_THAW_TOPIC"],   
                    Message=json.dumps(data))
      except ClientError as e:
        print(e)
        return abort(500)


@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
  #   pass
  
  # elif (request.method == 'POST'):
    # Process the subscription request

    # Create a customer on Stripe

    # Subscribe customer to pricing plan

    # Update user role in accounts database
    update_profile(identity_id=session['primary_identity'], role="premium_user")

    # Update role in the session
    session['role'] = "premium_user"

    # Request restoration of the user's data from Glacier
    # ...add code here to initiate restoration of archived user data
    # ...and make sure you handle files not yet archived!
    try:
      retrieve_data_from_glacier()
    except ClientError as e:
      print(e)
      return abort(500)

    # Display confirmation page
    return render_template('subscribe_confirm.html', stripe_id = app.config['STRIPE_UPGRADE_ID'])



"""Set premium_user role
"""
@app.route('/make-me-premium', methods=['GET'])
@authenticated
def make_me_premium():
  # Hacky way to set the user's role to a premium user; simplifies testing
  update_profile(
  identity_id=session['primary_identity'],
  role="premium_user"
  )
  return redirect(url_for('profile'))


"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
  identity_id=session['primary_identity'],
  role="free_user"
  )
  return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
  title='Page not found', alert_level='warning',
  message="The page you tried to reach does not exist. \
    Please check the URL and try again."
  ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
  title='Not authorized', alert_level='danger',
  message="You are not authorized to access this page. \
    If you think you deserve to be granted access, please contact the \
    supreme leader of the mutating genome revolutionary party."
  ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
  title='Not allowed', alert_level='warning',
  message="You attempted an operation that's not allowed; \
    get your act together, hacker!"
  ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
  title='Server error', alert_level='danger',
  message="The server encountered an error and could \
    not process your request."
  ), 500

### EOF
