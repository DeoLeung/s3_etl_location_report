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
      conn.row_factory = None
      c = conn.execute(
          """
          SELECT name
          FROM sqlite_master WHERE type='table' and name <> 'sqlite_sequence'
          """)
      golden = ['statistic', 'reported']
      self.assertEqual(sorted(golden), sorted(x[0] for x in c.fetchall()))
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
      conn.row_factory = None
      c = conn.execute("""SELECT * FROM statistic""")
      self.assertEqual(golden, c.fetchall())
      os.unlink(statistic_db.StatisticDB.DATABASE)

    def test_get_user_hourly_statistic(self):
      values = [
          (1384729205, u'user1', u'url1', u'Chrome', u'Windows Vista', False),
          (1384729285, u'user2', u'url1', u'Firefox', u'Windows XP', True)]
      golden = [(1384729200, 2, 2, 2, 1, 1)]
      tmp = tempfile.NamedTemporaryFile()
      statistic_db.StatisticDB.DATABASE = tmp.name
      tmp.close()
      db = statistic_db.StatisticDB()
      db.conn.row_factory = None
      db.insert_user_info(values)
      c = db.get_user_hourly_statistic()
      self.assertEqual(golden, c.fetchall())
      os.unlink(statistic_db.StatisticDB.DATABASE)

    def test_get_user_hourly_statistic_with_timestamp(self):
      values = [
          (1384729205, u'user1', u'url1', u'Chrome', u'Windows Vista', False),
          (1384729285, u'user2', u'url1', u'Firefox', u'Windows XP', True)]
      golden = [(1384729200, 2, 2, 2, 1, 1)]
      tmp = tempfile.NamedTemporaryFile()
      statistic_db.StatisticDB.DATABASE = tmp.name
      tmp.close()
      db = statistic_db.StatisticDB()
      db.conn.row_factory = None
      db.insert_user_info(values)
      c = db.get_user_hourly_statistic(1384729200)
      self.assertEqual(golden, c.fetchall())
      os.unlink(statistic_db.StatisticDB.DATABASE)

    def test_get_os_hourly_statistic(self):
      values = [
          (1384729205, u'user1', u'url1', u'Chrome', u'Windows Vista', False),
          (1384729285, u'user2', u'url1', u'Firefox', u'Windows XP', True)]
      golden = [
          (1384729200, 'Windows Vista', 1),
          (1384729200, 'Windows XP', 1)]
      tmp = tempfile.NamedTemporaryFile()
      statistic_db.StatisticDB.DATABASE = tmp.name
      tmp.close()
      db = statistic_db.StatisticDB()
      db.conn.row_factory = None
      db.insert_user_info(values)
      c = db.get_os_hourly_statistic()
      self.assertEqual(golden, c.fetchall())
      os.unlink(statistic_db.StatisticDB.DATABASE)

    def test_get_os_hourly_statistic_with_timestamp(self):
      values = [
          (1384729205, u'user1', u'url1', u'Chrome', u'Windows Vista', False),
          (1384724485, u'user2', u'url1', u'Firefox', u'Windows XP', True)]
      golden = [
          (1384729200, 'Windows Vista', 1)]
      tmp = tempfile.NamedTemporaryFile()
      statistic_db.StatisticDB.DATABASE = tmp.name
      tmp.close()
      db = statistic_db.StatisticDB()
      db.conn.row_factory = None
      db.insert_user_info(values)
      c = db.get_os_hourly_statistic(1384729200)
      self.assertEqual(golden, c.fetchall())
      os.unlink(statistic_db.StatisticDB.DATABASE)

    def test_get_browser_hourly_statistic(self):
      values = [
          (1384729205, u'user1', u'url1', u'Chrome', u'Windows Vista', False),
          (1384729285, u'user2', u'url1', u'Firefox', u'Windows XP', True)]
      golden = [
          (1384729200, 'Chrome', 1),
          (1384729200, 'Firefox', 1)]
      tmp = tempfile.NamedTemporaryFile()
      statistic_db.StatisticDB.DATABASE = tmp.name
      tmp.close()
      db = statistic_db.StatisticDB()
      db.conn.row_factory = None
      db.insert_user_info(values)
      c = db.get_browser_hourly_statistic()
      self.assertEqual(golden, c.fetchall())
      os.unlink(statistic_db.StatisticDB.DATABASE)

    def test_get_browser_hourly_statistic_with_timestamp(self):
      values = [
          (1384729205, u'user1', u'url1', u'Chrome', u'Windows Vista', False),
          (1385729285, u'user2', u'url1', u'Firefox', u'Windows XP', True)]
      golden = [
          (1384729200, 'Chrome', 1)]
      tmp = tempfile.NamedTemporaryFile()
      statistic_db.StatisticDB.DATABASE = tmp.name
      tmp.close()
      db = statistic_db.StatisticDB()
      db.conn.row_factory = None
      db.insert_user_info(values)
      c = db.get_browser_hourly_statistic(1384729200)
      self.assertEqual(golden, c.fetchall())
      os.unlink(statistic_db.StatisticDB.DATABASE)

    def test_update_reported(self):
      values = [
          (1384729205, u'user1', u'url1', u'Chrome', u'Windows Vista', False),
          (1385729285, u'user2', u'url1', u'Firefox', u'Windows XP', True)]
      tmp = tempfile.NamedTemporaryFile()
      statistic_db.StatisticDB.DATABASE = tmp.name
      tmp.close()
      db = statistic_db.StatisticDB()
      db.conn.row_factory = None
      db.insert_user_info(values)
      db.update_reported(1384729200, 800)
      result = db.get_connection().execute('SELECT * FROM reported').fetchall()
      self.assertEqual([(1384729200, 800), ], result)
      db.update_reported(1384729201, 799)
      db.update_reported(1384729200, 801)
      result = db.get_connection().execute('SELECT * FROM reported').fetchall()
      self.assertEqual([(1384729200, 801), (1384729201, 799)], result)
      os.unlink(statistic_db.StatisticDB.DATABASE)

    def test_get_unreported_hours(self):
      values = [
          (1384729205, u'user1', u'url1', u'Chrome', u'Windows Vista', False),
          (1384729205, u'user1', u'url3', u'Chrome', u'Windows Vista', False),
          (1380000010, u'user2', u'url1', u'Firefox', u'Windows XP', True)]
      tmp = tempfile.NamedTemporaryFile()
      statistic_db.StatisticDB.DATABASE = tmp.name
      tmp.close()
      db = statistic_db.StatisticDB()
      db.conn.row_factory = None
      db.insert_user_info(values)
      db.update_reported(1384729200, 1)
      result = db.get_unreported_hours()
      self.assertEqual([1379998800, 1384729200], result)
      os.unlink(statistic_db.StatisticDB.DATABASE)

if __name__ == '__main__':
    unittest.main()
