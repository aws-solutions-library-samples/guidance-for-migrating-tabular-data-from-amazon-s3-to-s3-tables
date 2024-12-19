import json
import cfnresponse
import logging
import os
import boto3
from botocore.exceptions import ClientError
from botocore.client import Config

# Enable debugging for troubleshooting
# boto3.set_stream_logger("")


# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


# Define Environmental Variables
my_region = str(os.environ['AWS_REGION'])


# Set SDK paramters
config = Config(retries = {'max_attempts': 5})

# Set variables
# Set Service Parameters
s3Client = boto3.client('s3', config=config, region_name=my_region)
glueClient = boto3.client('glue', region_name=my_region)


def get_table(db_name, tbl_name):
    logger.info(f"Checking if Source Glue Table Exists")
    try:
        check_table = glueClient.get_table(
            DatabaseName=db_name,
            Name=tbl_name,
        )
    except Exception as e:
        logger.error(e)
        raise e
    else:
        logger.info(check_table.get('Table').get('Name'))
        logger.info(f"Table {tbl_name} exists!")
        return check_table



def check_bucket_exists(bucket):
    logger.info(f"Checking if Source Bucket Exists")
    try:
        check_bucket = s3Client.get_bucket_location(
            Bucket=bucket,
        )
    except ClientError as e:
        logger.error(e)
        raise
    else:
        logger.info(f"Bucket {bucket}, exists, proceeding with deployment ...")
        return check_bucket            


def lambda_handler(event, context):
  # Define Environmental Variables
  s3Bucket  = event.get('ResourceProperties').get('bucketexists')
  gluedb = event.get('ResourceProperties').get('sourcedbexists')
  gluetbl = event.get('ResourceProperties').get('sourcetableexists')

  logger.info(f'Event detail is: {event}')

  if event.get('RequestType') == 'Create':
    # logger.info(event)
    try:
      logger.info("Stack event is Create, checking specified Source S3 Bucket and Source Glue Table exists...")
      if s3Bucket:
        check_bucket_exists(s3Bucket)
      get_table(gluedb, gluetbl)  
      responseData = {}
      responseData['message'] = "Successful"
      logger.info(f"Sending Invocation Response {responseData['message']} to Cloudformation Service")
      cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData)
    except Exception as e:
      logger.error(e)
      responseData = {}
      responseData['message'] = str(e)
      failure_reason = str(e) 
      logger.info(f"Sending Invocation Response {responseData['message']} to Cloudformation Service")
      cfnresponse.send(event, context, cfnresponse.FAILED, responseData, reason=failure_reason)


  elif event.get('RequestType') == 'Delete' or event.get('RequestType') == 'Update':
    logger.info(event)
    try:
      logger.info(f"Stack event is Delete or Update, nothing to do....")
      responseData = {}
      responseData['message'] = "Completed"
      logger.info(f"Sending Invocation Response {responseData['message']} to Cloudformation Service")
      cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData)
    except Exception as e:
      logger.error(e)
      responseData = {}
      responseData['message'] = str(e)
      logger.info(f"Sending Invocation Response {responseData['message']} to Cloudformation Service")
      cfnresponse.send(event, context, cfnresponse.FAILED, responseData)                  
