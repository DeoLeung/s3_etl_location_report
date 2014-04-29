import gzip
from StringIO import StringIO
from config import config
from pyspatialite import dbapi2 as spatialite
import logging
from user_agents import parse
import json
import gzip
import get_country_city_from_latlon
from config import config
import os


def process_log_file(key, bucket):
  logging.info('Processing %s', key.name)

  out = StringIO()
  processed_gzip = gzip.GzipFile(fileobj=out, mode="w")

  finder = get_country_city_from_latlon.LocationFinder()

  conn = spatialite.connect(
      os.path.join(config.DATA_FOLDER, config.STATISTIC_DATABASE))
  conn.text_factory = str
  c = conn.cursor()

  insert_values = []

  with gzip.GzipFile(fileobj=StringIO(key.get_contents_as_string())) as f:
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
      country, city = finder.getCountryAndCityFromLatLon(latitude, longitude)
      user_data = {
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
      processed_gzip.write(json.dumps(user_data) + '\n')
      insert_values.append(
          (int(timestamp), user_id, url,
           user_agent.browser.family, user_agent.os.family, user_agent.is_mobile))
  c.executemany(
      """
      INSERT INTO statistic(
        timestamp, user, url, browser_family, os_family, is_mobile)
      VALUES (?, ?, ?, ?, ?, ?)
      """, insert_values)
  conn.commit()

  k = bucket.new_key(key.name.replace(config.LOG_DIR, config.PROCESSED_DIR))
  k.set_contents_from_string(out.getvalue())
  out.close()
  k.close()
  finder.close()
  # total users, url
  c.execute('select count(*) from statistic')
  print c.fetchall()
  # unique users
  c.execute('select count(distinct user) from statistic')
  print c.fetchall()
  # unique url
  c.execute('select count(distinct url) from statistic')
  print c.fetchall()
  # os family stats
  c.execute('select os_family, count(os_family) from statistic group by os_family')
  print c.fetchall()
  # browser family stats
  c.execute('select browser_family, count(browser_family) from statistic group by browser_family')
  print c.fetchall()
  # is mobile
  c.execute('select is_mobile, count(is_mobile) from statistic group by is_mobile')
  print c.fetchall()
  conn.close()
  logging.info('Finish processing %s, uploaded to %s', key.name, k.name)
