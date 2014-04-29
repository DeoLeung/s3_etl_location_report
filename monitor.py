import gzip
import json
import os
from StringIO import StringIO
import logging
from user_agents import parse

from config import config
import location_db
import s3_service
import statistic_db

class Monitor(object):

  def extract(self, content):
    location = location_db.LocationDB()
    with gzip.GzipFile(fileobj=StringIO(content)) as f:
      for line in f:
        try:
          (timestamp, user_id, latitude_longitude,
           url, user_agent_string) = line.strip().split('\t')
        except ValueError:
          logging.warn('Error in parsing line: %s', line.strip())
          continue
        try:
          latitude, longitude = latitude_longitude.strip('[]').split(',')
        except ValueError as msg:
          logging.warn('Error in parsing latlon: %s from line: %s',
                       latitude_longitude, line.strip())
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
      logging.info('Processing %s', key.name)
      out = StringIO()
      processed_gzip = gzip.GzipFile(fileobj=out, mode="w")
      new_records = []
      for record in self.extract(service.download(key)):
        processed_gzip.write(json.dumps(record) + '\n')
        new_records.append(
            (int(record['timestamp']), record['user_id'], record['url'],
             record['user_agent']['browser_family'],
             record['user_agent']['os_family'],
             record['user_agent']['mobile']))
      # TODO: implement proper rollback if this fails.
      logging.info('Inserting %d records to statistic database', len(new_records))
      statistic = statistic_db.StatisticDB()
      statistic.insert_user_info(new_records)
      statistic.close()
      logging.info('Finish inserting records to statistic database')
      new_key = key.name.replace(config.LOG_DIR, config.PROCESSED_DIR)
      service.upload(new_key, out.getvalue())
      out.close()
      logging.info('Finish processing %s, uploaded to %s', key.name, new_key)
