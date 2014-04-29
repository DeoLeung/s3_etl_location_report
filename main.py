import logging
import time

from config import config
import monitor
import report

def main():
  logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
  monitor_s3 = monitor.Monitor()
  report_s3 = report.Report()
  while True:
    # TODO: better make it multi-threaded once the locking of database is
    #       handled.
    start = int(time.time())
    monitor_s3.run()
    end = int(time.time())
    report_s3.run()
    #time.sleep(config.S3_MONITOR_TIME - (start + end))

if __name__=='__main__':
  main()
