import os
from pyspatialite import dbapi2 as spatialite
from config import config

class LocationFinder(object):

  def __init__(self):
    self.conn = spatialite.connect(
        os.path.join(config.DATA_FOLDER, config.CITIES_DATABASE))
    self.c = self.conn.cursor()

  def getCountryAndCityFromLatLon(self, latitude, longitude):
    self.c.execute(
        """
        SELECT
          city, country, DISTANCE(geomFromText('POINT(%s %s)', 4326), geometry)
        FROM cities
        WHERE cities.ROWID IN (
          SELECT pkid
          FROM idx_cities_geometry
          WHERE pkid MATCH RTreeDistWithin(%s, %s, 1))
        ORDER BY 3
        """ % (longitude, latitude, longitude, latitude))
    result = self.c.fetchone()
    if result:
      return result[1], result[0]
    else:
      return None, None

  def close(self):
    self.conn.close()
