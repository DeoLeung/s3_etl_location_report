import logging
import time
import threading
import sys

from config import config
import monitor
import report

def run_repeatly(func, period):
  start = time.time()
  func()
  end = time.time()
  wait = period - (end - start)
  wait = wait if wait > 0 else 0
  print '%s is waiting %d' % (func, wait)
  threading.Timer(wait, run_repeatly, [func, period]).start()

def main():
  logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
  monitor_s3 = monitor.Monitor()
  report_s3 = report.Report()
  t1 = threading.Thread(
      target=run_repeatly, args=[monitor_s3.run, config.S3_MONITOR_TIME])
  t2 = threading.Thread(
      target=run_repeatly, args=[report_s3.run, config.S3_REPORT_TIME])
  t1.start()
  t2.start()
  t1.join()
  t2.join()

if __name__=='__main__':
  try:
    main()
  except KeyboardInterrupt:
    pass
