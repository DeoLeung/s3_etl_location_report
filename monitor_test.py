import unittest

import monitor

class TestMonitor(unittest.TestCase):

  def setUp(self):
    self.monitor = monitor.Monitor()

  def test_extract(self):
    golden = [
        {'url': 'http://www.destinology.co.uk/mauritius/hotel/'
                'four-seasons-resort-at-anahita/14408/',
         'timestamp': 1398193201,
         'user_id': '609e1256424ea308bd595db13227c1c2',
         'location': {
            'latitude': '51.50',
            'longitude': '-0.30',
            'country': 'United Kingdom',
            'city': 'Hanwell'},
          'user_agent': {
            'mobile': False,
            'os_family': u'Windows 7',
            'string': 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1;'
                      ' WOW64; Trident/5.0)',
            'browser_family': u'IE'}}]
    with open('./testdata/log.gz') as f:
      for r, g in zip(self.monitor.extract(f.read()), golden):
        self.assertEqual(r, g)

  def test_validate_timestamp(self):
    values = [
      ('-1', False),
      ('1', True),
      ('99999999999999999999999999', True),
      ('1.2', False),
      ('wednesday', False)]
    for i, g in values:
      self.assertEqual(self.monitor.validate_timestamp(i), g)

  def test_validate_latitude(self):
    values = [
      ('-1', True),
      ('90.1', False),
      ('99999999999999999999999999', False),
      ('1.2', True),
      ('wednesday', False)]
    for i, g in values:
      self.assertEqual(self.monitor.validate_latitude(i), g)

  def test_validate_longitude(self):
    values = [
      ('-1', True),
      ('90.1', True),
      ('180.1', False),
      ('99999999999999999999999999', False),
      ('1.2', True),
      ('wednesday', False)]
    for i, g in values:
      self.assertEqual(self.monitor.validate_longitude(i), g)

  def test_validate_url(self):
    values = [
      ('-1', False),
      ('weibo.com', False),
      ('http://weibo.com', True),
      ('www.weibo.com', False)]
    for i, g in values:
      self.assertEqual(self.monitor.validate_url(i), g)

if __name__ == '__main__':
    unittest.main()
