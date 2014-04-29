import logging
from datetime import datetime
from config import config
import s3_monitor
import process_logs

def main():
  logging.basicConfig(level=logging.INFO)
  #last_monitored = datetime.strptime('2014-04-28T00:00:00.000Z', config.TIMESTAMP_FORMAT)
  keys, bucket, conn = s3_monitor.get_new_keys()
  logging.info('Found %d new log files', len(keys))
  if keys:
    for key in keys:
      process_logs.process_log_file(key, bucket)
  conn.close()

if __name__=='__main__':
  main()
