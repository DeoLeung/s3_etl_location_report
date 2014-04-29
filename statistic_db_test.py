import os
import sqlite3
import tempfile
import unittest

import statistic_db

class TestStatisticDB(unittest.TestCase):

    def test_init_if_db_not_exists(self):
      tmp = tempfile.NamedTemporaryFile()
      statistic_db.StatisticDB.DATABASE = tmp.name
      tmp.close()
      db = statistic_db.StatisticDB()
      conn = db.get_connection()
      c = conn.execute(
          """
          SELECT name FROM sqlite_master WHERE type='table' AND name='statistic'
          """)
      self.assertTrue(c.fetchall())
      c = conn.execute(
          """
          SELECT name FROM sqlite_master WHERE type='view'
          """)
      golden = [
          ('os_family_hourly',),
          ('user_general_hourly',),
          ('browser_family_hourly',)]
      self.assertEqual(sorted(golden), sorted(c.fetchall()))
      os.unlink(statistic_db.StatisticDB.DATABASE)

    def test_init_if_db_exists(self):
      statistic_db.StatisticDB.DATABASE = (
          './testdata/initial_statistic.sqlite3.db')
      db = statistic_db.StatisticDB()
      conn = db.get_connection()
      c = conn.execute(
          """
          SELECT name FROM sqlite_master WHERE type='table' AND name='statistic'
          """)
      self.assertTrue(c.fetchall())

    def test_insert_user_info(self):
      values = [
          (1384729205, u'user1', u'url1', u'Chrome', u'Windows Vista', False),
          (1384729285, u'user2', u'url2', u'Firefox', u'Windows XP', True)]
      golden = [
          (1, 1384729205, 'user1', 'url1', 'Chrome', 'Windows Vista', 0),
          (2, 1384729285, 'user2', 'url2', 'Firefox', 'Windows XP', 1)]
      tmp = tempfile.NamedTemporaryFile()
      statistic_db.StatisticDB.DATABASE = tmp.name
      tmp.close()
      db = statistic_db.StatisticDB()
      db.insert_user_info(values)
      conn = db.get_connection()
      c = conn.execute("""SELECT * FROM statistic""")
      self.assertEqual(golden, c.fetchall())
      os.unlink(statistic_db.StatisticDB.DATABASE)

if __name__ == '__main__':
    unittest.main()