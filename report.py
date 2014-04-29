from datetime import datetime

from config import config
import statistic_db

def generate_report(bucket):
  data = [(
      'date & time',
      'number of unique users',
      'number of total users',
      'number of unique domains',
      'number of total domains',
      'browser family stats',
      'os family stats',
      'desktop/ mobile counts and ratios')]

  statistic = statistic_db.StatisticDB()
  c = statistic.get_user_hourly_statistic()
  file_name = None
  last_day = 0
  for result in c.fetchall():
    timestamp = datetime.fromtimestamp(result['hour'])
    if last_day != timestamp.day:
      if file_name:
        k = bucket.new_key(key.name.replace(config.LOG_DIR, config.REPORTING_DIR))
        k.set_contents_from_string(out.getvalue())
        out.close()
        k.close()
    os_stat = []
    for os in statistic.get_os_hourly_statistic(result['hour']).fetchall():
      os_stat.append(
          (os['os_family'], os['quantity'] * 1.0 / result['total_users']))
    os_stat.sort(key=lambda x: x[1], reverse=True)
    browser_stat = []
    for browser in statistic.get_browser_hourly_statistic(result['hour']).fetchall():
      browser_stat.append(
          (browser['browser_family'],
           browser['quantity'] * 1.0 / result['total_users']))
    browser_stat.sort(key=lambda x: x[1], reverse=True)
    data.append((
        timestamp.isoformat(' '),
        str(result['distinct_users']),
        str(result['total_users']),
        str(result['distinct_urls']),
        str(result['total_urls']),
        ';'.join('%s,%f' % (n, f) for n, f in os_stat),
        ';'.join('%s,%f' % (n, f) for n, f in browser_stat)))
    print '\n'.join(data[1])
    break

if __name__=='__main__':
  generate_report()
