import csv
import os
from pyspatialite import dbapi2 as spatialite
from config import config

if not os.path.exists(config.DATA_FOLDER):
  os.makedirs(config.DATA_FOLDER)

with open(config.CITIES_CSV_LATLON) as f:
  reader = csv.reader(f, delimiter=',')
  header = next(reader, None)
  geoname_id_idx = header.index('geoname_id')
  latitude_idx = header.index('latitude')
  longitude_idx = header.index('longitude')
  geoname_id_to_latlon = {}
  for l in reader:
    geoname_id, lat, lon = l[geoname_id_idx], l[latitude_idx], l[longitude_idx]
    if not (geoname_id and lat and lon):
      continue
    if geoname_id not in geoname_id_to_latlon or geoname_id_to_latlon[geoname_id] == (lat, lon):
      geoname_id_to_latlon[geoname_id] = (lat, lon)

with open(config.CITIES_CSV_NAME) as f:
  reader = csv.reader(f, delimiter=',')
  header = next(reader, None)
  geoname_id_idx = header.index('geoname_id')
  country_name_idx = header.index('country_name')
  city_name_idx = header.index('city_name')
  geoname_id_to_names = {}
  for l in reader:
    geoname_id, country_name, city_name = l[geoname_id_idx], l[country_name_idx], l[city_name_idx]
    if not (geoname_id and country_name and city_name):
      continue
    if geoname_id not in geoname_id_to_names or geoname_id_to_names[geoname_id] == (city_name, country_name):
      geoname_id_to_names[geoname_id] = (city_name, country_name)

cities = []
for geoname_id, (city_name, country_name) in geoname_id_to_names.iteritems():
  if geoname_id not in geoname_id_to_latlon:
    print geoname_id
    continue
  latitude, longitude = geoname_id_to_latlon[geoname_id]
  cities.append((city_name, country_name, latitude, longitude))

conn = spatialite.connect(
    os.path.join(config.DATA_FOLDER, config.CITIES_DATABASE))
c = conn.cursor()
conn.text_factory = str
c.execute("""SELECT InitSpatialMetaData()""")
c.execute(
    """
    CREATE TABLE cities(
      PKUID INTEGER PRIMARY KEY autoincrement,
      city text NOT NULL,
      country text NOT NULL)
    """)
c.execute(
    """SELECT AddGeometryColumn('cities', 'geometry', 4326, 'POINT', 2)""")
for city_name, country_name, latitude, longitude in cities:
  c.execute("insert into cities (city, country, geometry)"
            "  values (?, ?, geomFromText('POINT(%s %s)', 4326))"
            % (longitude, latitude),
            (city_name, country_name))
conn.commit()
c.execute("SELECT CreateSpatialIndex('cities', 'geometry')")
conn.close()

# statistic database
conn = spatialite.connect(
    os.path.join(config.DATA_FOLDER, config.STATISTIC_DATABASE))
conn.text_factory = str
c = conn.cursor()
# Create table
c.execute(
    """
    CREATE TABLE statistic (
        PKUID INTEGER PRIMARY KEY autoincrement,
        timestamp INTEGER,
        user text,
        url text,
        browser_family text,
        os_family text,
        is_mobile bool)
    """)
conn.close()
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
