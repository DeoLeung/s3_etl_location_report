import os
from pyspatialite import dbapi2 as spatialite
import tempfile
import unittest

import location_db

class TestLocationDB(unittest.TestCase):

    def test_init_if_db_not_exists(self):
      tmp = tempfile.NamedTemporaryFile()
      location_db.LocationDB.DATABASE = tmp.name
      location_db.LocationDB.CITIES_CSV_LATLON = './testdata/blocks.csv'
      location_db.LocationDB.CITIES_CSV_NAME = './testdata/location.csv'
      tmp.close()
      db = location_db.LocationDB()
      conn = db.get_connection()
      c = conn.execute(
          """
          SELECT name FROM sqlite_master WHERE type='table' AND name='cities'
          """)
      self.assertTrue(c.fetchall())
      os.unlink(location_db.LocationDB.DATABASE)

    def test_init_if_db_exists(self):
      location_db.LocationDB.DATABASE = './testdata/location.spatialite.db'
      db = location_db.LocationDB()
      conn = db.get_connection()
      c = conn.execute(
          """
          SELECT name FROM sqlite_master WHERE type='table' AND name='cities'
          """)
      self.assertTrue(c.fetchall())

    def test_get_latlon(self):
      location_db.LocationDB.DATABASE = './testdata/location.spatialite.db'
      db = location_db.LocationDB()
      self.assertEqual(
          ('China', 'Guangzhou'),
          db.get_country_and_city_from_lat_lon(35.6900, 139.7000))
      self.assertEqual(
          ('Japan', 'Tokyo'),
          db.get_country_and_city_from_lat_lon(23.5067,113.2500))
      self.assertEqual(
          (None, None),
          db.get_country_and_city_from_lat_lon(24.5067,113.2500))

if __name__ == '__main__':
    unittest.main()
