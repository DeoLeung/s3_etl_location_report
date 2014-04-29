"""All operations relate to location.spatialite.db."""
import csv
import os
from pyspatialite import dbapi2 as spatialite

from config import config

class LocationDB(object):
  """A class for operations to location.spatialite.db."""

  DATABASE = os.path.join(config.DATA_FOLDER, config.LOCATION_DATABASE)
  CITIES_CSV_LATLON = config.CITIES_CSV_LATLON
  CITIES_CSV_NAME = config.CITIES_CSV_NAME

  def __init__(self):
    self._create_database()
    self.connect()

  def connect(self):
    """Open a connection to database.

    If database is missing, create it before opening.
    """
    self.conn = spatialite.connect(self.DATABASE)
    self.conn.text_factory = str

  def close(self):
    """Close a connection to database."""
    self.conn.close()

  def get_connection(self):
    return self.conn

  def _create_database(self):
    if os.path.exists(self.DATABASE):
      # Do nothing if the database exists.
      return
    # Create the table.
    with open(self.CITIES_CSV_LATLON) as f:
      reader = csv.reader(f, delimiter=',')
      header = next(reader, None)
      geoname_id_idx = header.index('geoname_id')
      latitude_idx = header.index('latitude')
      longitude_idx = header.index('longitude')
      geoname_id_to_latlon = {}
      for l in reader:
        geoname_id = l[geoname_id_idx]
        lat = l[latitude_idx]
        lon = l[longitude_idx]
        if not (geoname_id and lat and lon):
          continue
        if (geoname_id not in geoname_id_to_latlon or
            geoname_id_to_latlon[geoname_id] == (lat, lon)):
          geoname_id_to_latlon[geoname_id] = (lat, lon)

    with open(self.CITIES_CSV_NAME) as f:
      reader = csv.reader(f, delimiter=',')
      header = next(reader, None)
      geoname_id_idx = header.index('geoname_id')
      country_name_idx = header.index('country_name')
      city_name_idx = header.index('city_name')
      geoname_id_to_names = {}
      for l in reader:
        geoname_id = l[geoname_id_idx]
        country_name = l[country_name_idx]
        city_name = l[city_name_idx]
        if not (geoname_id and country_name and city_name):
          continue
        if (geoname_id not in geoname_id_to_names or
            geoname_id_to_names[geoname_id] == (city_name, country_name)):
          geoname_id_to_names[geoname_id] = (city_name, country_name)

    cities = []
    for geoname_id, (city, country) in geoname_id_to_names.iteritems():
      if geoname_id not in geoname_id_to_latlon:
        print geoname_id
        continue
      latitude, longitude = geoname_id_to_latlon[geoname_id]
      cities.append((city, country, latitude, longitude))

    conn = spatialite.connect(self.DATABASE)
    conn.text_factory = str
    conn.execute("""SELECT InitSpatialMetaData()""")
    conn.execute(
        """
        CREATE TABLE cities(
          PKUID INTEGER PRIMARY KEY autoincrement,
          city text NOT NULL,
          country text NOT NULL)
        """)
    conn.execute(
        """SELECT AddGeometryColumn('cities', 'geometry', 4326, 'POINT', 2)""")
    for city_name, country_name, latitude, longitude in cities:
      # TODO: use executemany once getting rid of %s
      conn.execute("insert into cities (city, country, geometry)"
                "  values (?, ?, geomFromText('POINT(%s %s)', 4326))"
                % (longitude, latitude),
                (city_name, country_name))
    conn.commit()
    conn.execute("SELECT CreateSpatialIndex('cities', 'geometry')")
    conn.close()

  def get_country_and_city_from_lat_lon(self, latitude, longitude, ratio=1):
    c = self.conn.execute(
        """
        SELECT
          city, country, DISTANCE(geomFromText('POINT(%s %s)', 4326), geometry)
        FROM cities
        WHERE cities.ROWID IN (
          SELECT pkid
          FROM idx_cities_geometry
          WHERE pkid MATCH RTreeDistWithin(%s, %s, %s))
        ORDER BY 3
        """ % (longitude, latitude, longitude, latitude, ratio))
    result = c.fetchone()
    if result:
      return result[1], result[0]
    else:
      return None, None


# some data from somewhere....
"""
test_data = [
('FR', 'Thomas', 45.3833, 2.5),
('GB', 'Pelton', 54.8667, -1.6),
('GB', 'Seaham', 54.8367, -1.3381),
('RU', 'Ivanovo', 56.9972, 40.9714),
('BE', 'Ensival', 50.5833, 5.85),
('FR', 'Sommi?res', 46.2791, 0.3607),
('GB', 'Constantine', 50.1167, -5.1667)]
"""
