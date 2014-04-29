import calendar
from datetime import datetime
import gzip
import logging
import os
from StringIO import StringIO

from config import config
import statistic_db
import s3_service

class Report(object):

  def __init__(self):
    self.header = (
        'date & time',
        'number of unique users',
        'number of total users',
        'number of unique domains',
        'number of total domains',
        'browser family stats',
        'os family stats',
        'desktop/ mobile counts and ratios')

  def run(self):
    statistic = statistic_db.StatisticDB()
    dates = set()
    for timestamp in statistic.get_unreported_hours():
      date = datetime.fromtimestamp(timestamp)
      dates.add((date.year, date.month, date.day))
    if not timestamp:
      return
    service = s3_service.S3Service()
    for date in dates:
      key = os.path.join(
          config.REPORTING_DIR, '%d/%d/%d' % date, 'report.gz')
      logging.info('Generating report to %s', key)
      out = StringIO()
      report_gzip = gzip.GzipFile(fileobj=out, mode="w")
      report_gzip.write('\t'.join(self.header) + '\n')
      timestamp = calendar.timegm(datetime(*date).utctimetuple()) - 3600
      num_of_records = 0
      for i in xrange(24):
        timestamp += 3600
        result = statistic.get_user_hourly_statistic(timestamp).fetchone()
        if not result:
          continue
        num_of_records += 1
        os_stat = []
        for os_family in statistic.get_os_hourly_statistic(result['hour']).fetchall():
          os_stat.append(
              (os_family['os_family'],
               os_family['quantity'] * 1.0 / result['total_users']))
        os_stat.sort(key=lambda x: x[1], reverse=True)
        browser_stat = []
        for browser in statistic.get_browser_hourly_statistic(result['hour']).fetchall():
          browser_stat.append(
              (browser['browser_family'],
               browser['quantity'] * 1.0 / result['total_users']))
        browser_stat.sort(key=lambda x: x[1], reverse=True)
        report_gzip.write(
            '\t'.join([
                datetime.fromtimestamp(timestamp).isoformat(' '),
                str(result['distinct_users']),
                str(result['total_users']),
                str(result['distinct_urls']),
                str(result['total_urls']),
                ';'.join('%s,%f' % (n, f) for n, f in os_stat),
                ';'.join('%s,%f' % (n, f) for n, f in browser_stat)]) + '\n')
        # TODO: shall update this in one go for a whole day.
        statistic.update_reported(timestamp, result['total_users'])
      logging.info(
          'Finished generating report %s with %d records', key, num_of_records)
      service.upload(key, out.getvalue())
      out.close()
    service.close()
