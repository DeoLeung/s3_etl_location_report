module etup:
Download city latlon data from
http://geolite.maxmind.com/download/geoip/database/GeoLite2-City-CSV.zip
extract it to a subfolder under working path, e.g. data/ or update DATA in
config/config.py
update the path after data/ of CITIES_CSV_LATLON and CITIES_CSV_NAME in
config/config.py if necessary.

Install pyspatialite
please see https://github.com/lokkju/pyspatialite/issues/18 as there is some
issue about getting the module work.

Install user-agents parser
see https://pypi.python.org/pypi/user-agents/0.2.0

Install aws s3 python interface boto
https://aws.amazon.com/sdkforpython/

usage:
cd directory_of_the_code
python main.py

configuration:
see config/config.py
