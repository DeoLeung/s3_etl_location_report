from boto.s3.connection import S3Connection
import config.config as config
import logging
import sys
from datetime import datetime

def get_new_keys():
  # Creates a S3 connection
  conn = S3Connection(config.S3_ACCESS_KEY_ID, config.S3_SECRET_ACCESS_KEY)
  try:
    bucket = conn.get_bucket(config.BUCKET_NAME)
  except S3ResponseError as msg:
    logging.critical('Failed to connect to bucket: %s', config.BUCKET_NAME)
    sys.exit(1)
  logging.info('Connected to bucket: %s', config.BUCKET_NAME)

  processed_keys = {
      k.name.replace(config.PROCESSED_DIR, '')
      for k in bucket.list(prefix=config.PROCESSED_DIR)}
  new_keys = []
  for key in bucket.list(prefix=config.LOG_DIR):
    if key.name.replace(config.LOG_DIR, '') not in processed_keys:
      new_keys.append(key)

  # TODO: approach using timestamp, need to solve the last time record problem.
  #for key in bucket.list(prefix=config.LOG_DIR):
    #last_modified = datetime.strptime(
        #key.last_modified, config.TIMESTAMP_FORMAT)
    #if last_modified > last_monitored:
      #new_keys.append(key)

  return new_keys, bucket, conn
