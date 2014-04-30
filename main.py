import logging
from multiprocessing import Process
from threading import Timer
import time

from config import config
import monitor
import report

def run_repeatly(func, period):
  start = time.time()
  func()
  end = time.time()
  wait = period - (end - start)
  wait = wait if wait > 0 else 0
  logging.debug('%s is waiting %d', func.__name__, wait)
  Timer(wait, run_repeatly, [func, period]).start()

def main():
  logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
  monitor_s3 = monitor.Monitor()
  report_s3 = report.Report()
  p1 = Process(
      target=run_repeatly, args=[monitor_s3.run, config.S3_MONITOR_TIME])
  p2 = Process(
      target=run_repeatly, args=[report_s3.run, config.S3_REPORT_TIME])
  p1.daemon = True
  p2.daemon = True
  p1.start()
  p2.start()
  p1.join()
  p2.join()

if __name__=='__main__':
  try:
    main()
  except KeyboardInterrupt:
    logging.info('Exit program as ctrl-c is pressed')
