import logging
from multiprocessing import Process
from threading import Timer
import time

from config import config
import monitor
import report

def run_repeatly(func, period):
  """Run the function periodically.

  It runs the function, then wait period subtracting the running time, if
  running time exceeds the period, the next run starts next period.
  Args:
    func: the function to be run
    period: the period between runs
  """
  start = time.time()
  func()
  end = time.time()
  wait = period - (end - start)
  if wait < 0:
    # adjust to next period
    wait = period -(-wait % period)
  logging.info('%s is waiting %ds', func.__name__, wait)
  t = Timer(wait, run_repeatly, [func, period])
  t.start()
  t.join()

def main():
  logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
  # TODO: the 2 service don't really maintain stages, make them static
  monitor_s3 = monitor.Monitor()
  report_s3 = report.Report()
  p1 = Process(
      target=run_repeatly, args=(monitor_s3.run, config.S3_MONITOR_TIME))
  p2 = Process(
      target=run_repeatly, args=(report_s3.run, config.S3_REPORT_TIME))
  # Terminate them if the main program is terminated
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
