import gzip
import json
from StringIO import StringIO
import logging
from urlparse import urlparse
from user_agents import parse

from config import config
import location_db
import s3_service
import statistic_db

class Monitor(object):

  def validate_timestamp(self, s):
    try:
      result = int(s)
    except ValueError:
      return False
    if result < 0:
      return False
    return True

  def validate_latitude(self, s):
    try:
      result = float(s)
    except ValueError:
      return False
    if -90.0001 < result < 90.0001:
      return True
    return False

  def validate_longitude(self, s):
    try:
      result = float(s)
    except ValueError:
      return False
    if -180.0001 < result < 180.0001:
      return True
    return False

  def validate_url(self, s):
    parts = urlparse(s)
    if not parts.scheme or not parts.netloc:
      # naive validation, more heavily use rfc3987
      return False
    return True

  def extract(self, content):
    """Extract gzipped data into formatted record.

    Args:
      content: gizpped data as binary string
    Yields:
      a dict of one record of data
    """
    location = location_db.LocationDB()
    with gzip.GzipFile(fileobj=StringIO(content)) as f:
      for line in f:
        try:
          (timestamp, user_id, latitude_longitude,
           url, user_agent_string) = line.strip().split('\t')
        except ValueError:
          logging.warn('Error in parsing line: %s', line.strip())
          continue
        if not self.validate_timestamp(timestamp):
          logging.warn('Error in parsing timestamp %s in line: %s',
                       timestamp, line.strip())
          continue
        if not self.validate_url(url):
          logging.warn('Invalid url %s in line: %s',
                       url, line.strip())
          continue
        try:
          latitude, longitude = latitude_longitude.strip('[]').split(',')
        except ValueError as msg:
          logging.warn('Error in parsing latlon: %s from line: %s',
                       latitude_longitude, line.strip())
          continue
        if not self.validate_latitude(latitude):
          logging.warn('Invalid latitude %s in line: %s',
                       latitude, line.strip())
          continue
        if not self.validate_longitude(longitude):
          logging.warn('Invalid longitude %s in line: %s',
                       longitude, line.strip())
          continue
        user_agent = parse(user_agent_string)
        country, city = location.get_country_and_city_from_lat_lon(latitude,
                                                                   longitude)
        yield {
            'url': url,
            'timestamp': int(timestamp),
            'user_id': user_id,
            'location': {
                'latitude': latitude,
                'country': country,
                'longitude': longitude,
                'city': city},
            'user_agent': {
                'mobile': user_agent.is_mobile,
                'os_family': user_agent.os.family,
                'string': user_agent_string,
                'browser_family': user_agent.browser.family}}
    location.close()

  def run(self):
    service = s3_service.S3Service()
    for key in service.get_new_logs():
      # process the new keys
      out = StringIO()
      processed_gzip = gzip.GzipFile(fileobj=out, mode="w")
      new_records = []
      logging.info('Processing %s', key.name)
      for record in self.extract(service.download(key)):
        # process every line from downloaded content
        processed_gzip.write(json.dumps(record) + '\n')
        new_records.append(
            (int(record['timestamp']), record['user_id'], record['url'],
             record['user_agent']['browser_family'],
             record['user_agent']['os_family'],
             record['user_agent']['mobile']))
      # TODO: implement proper rollback if this fails.
      logging.info(
          'Inserting %d records to statistic database', len(new_records))
      # update new user info into database
      statistic = statistic_db.StatisticDB()
      statistic.insert_user_info(new_records)
      statistic.close()
      logging.info('Finish inserting records to statistic database')
      # upload the processed file to s3
      new_key = key.name.replace(config.LOG_DIR, config.PROCESSED_DIR)
      processed_gzip.close()
      service.upload(new_key, out.getvalue())
      out.close()
      logging.info('Finish processing %s', key.name)
    service.close()
