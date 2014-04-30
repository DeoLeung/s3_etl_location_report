from boto.s3.connection import S3Connection
from boto import exception
import logging
import sys

import config.config as config

class S3Service(object):

  def __init__(self):
    # Creates a S3 connection
    self.conn = S3Connection(
        config.S3_ACCESS_KEY_ID, config.S3_SECRET_ACCESS_KEY)
    try:
      self.bucket = self.conn.get_bucket(config.BUCKET_NAME)
    except exception.S3ResponseError as msg:
      logging.critical('Failed to connect to bucket: %s', config.BUCKET_NAME)
      sys.exit(1)
    logging.info('Connected to bucket: %s', config.BUCKET_NAME)

  def __enter__(self):
    self

  def __exit__(self, type, value, trackback):
    self.close()

  def get_new_logs(self):
    processed_keys = {
        k.name.replace(config.PROCESSED_DIR, '')
        for k in self.bucket.list(prefix=config.PROCESSED_DIR)}
    new_keys = []
    for key in self.bucket.list(prefix=config.LOG_DIR):
      if key.name.replace(config.LOG_DIR, '') not in processed_keys:
        new_keys.append(key)
    logging.info('Found %d new log files', len(new_keys))
    return new_keys

  def upload(self, key, content):
    logging.info('Uploading %s', key)
    k = self.bucket.new_key(key)
    k.set_contents_from_string(content)
    k.close()
    logging.info('Uploaded %s', key)

  def download(self, key):
    logging.info('Downloading %s', key.name)
    content = key.get_contents_as_string()
    logging.info('Downloaded %s', key.name)
    return content

  def close(self):
    self.conn.close()
    logging.info('Closed connection with S3')

