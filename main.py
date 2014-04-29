import logging

from config import config
import monitor

def main():
  logging.basicConfig(level=logging.INFO)
  monitor_s3 = monitor.Monitor()
  monitor_s3.run()

if __name__=='__main__':
  main()
