# S3 credentials
S3_USER_NAME = 'xy'
S3_ACCESS_KEY_ID = 'jk'
S3_SECRET_ACCESS_KEY = 'lmn'

# s3 bucket configuation
BUCKET_NAME = 'op'
LOG_DIR = 'q'
PROCESSED_DIR = 'r' % S3_USER_NAME
REPORTING_DIR = 's' % S3_USER_NAME

# Notice the last timezone is interpreted as 'Z' explicitely.
TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
THIRD_PARTY_FOLDER = 'third_party'
DATA_FOLDER = 'data'
CITIES_DATABASE = 'cities.spatialite.db'
STATISTIC_DATABASE = 'statistic.spatialite.db'
CITIES_CSV_LATLON = 'GeoLite2-City-CSV_20140401/GeoLite2-City-Blocks.csv'
CITIES_CSV_NAME = 'GeoLite2-City-CSV_20140401/GeoLite2-City-Locations.csv'
