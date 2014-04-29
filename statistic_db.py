"""All operations relate to the statistic.sqlite3.db."""
import os
import sqlite3

from config import config


class StatisticDB(object):
  """A class for operations to statistic.sqlite.db."""

  DATABASE = os.path.join(config.DATA_FOLDER, config.STATISTIC_DATABASE)

  def __init__(self):
    self._create_database()
    self.connect()

  def connect(self):
    """Open a connection to database.

    If database is missing, create it before opening.
    """
    self.conn = sqlite3.connect(self.DATABASE)
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
    conn = sqlite3.connect(self.DATABASE)
    # Create the table.
    conn.execute(
        """
        CREATE TABLE statistic (
            PKUID INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER,
            user TEXT NOT NULL,
            url TEXT NOT NULL,
            browser_family TEXT NOT NULL,
            os_family TEXT NOT NULL,
            is_mobile BOOL)
        """)
    # Create views.
    conn.execute(
        """
        CREATE VIEW user_general_hourly AS
        SELECT
          timestamp/3600*3600 AS hour,
          COUNT(user) AS total_users,
          COUNT(distinct user) AS distinct_users,
          COUNT(url) AS total_urls,
          COUNT(distinct url) AS distinct_urls,
          SUM(is_mobile) AS num_mobile
        FROM statistic
        GROUP BY hour
        """)
    conn.execute(
        """
        CREATE VIEW os_family_hourly AS
        SELECT
          timestamp/3600*3600 AS hour,
          os_family,
          COUNT(os_family) AS quantity
        FROM statistic
        GROUP BY hour, os_family
        """)
    conn.execute(
        """
        CREATE VIEW browser_family_hourly AS
        SELECT
          timestamp/3600*3600 AS hour,
          browser_family,
          COUNT(browser_family) AS quantity
        FROM statistic
        GROUP BY hour, browser_family
        """)
    conn.close()

  def insert_user_info(self, values):
    # TODO: implement the rollback for this operation.
    self.conn.executemany(
        """
        INSERT INTO statistic (
          timestamp, user, url, browser_family, os_family, is_mobile)
        VALUES (?, ?, ?, ?, ?, ?)
        """, values)
    self.conn.commit()

  def get_user_hourly_statistic(self, timestamp=None):
    if timestamp:
      return self.conn.execute(
          """SELECT * FROM user_general_hourly WHERE hour=?""", (timestamp,))
    return self.conn.execute(
        """SELECT * FROM user_general_hourly ORDER BY hour DESC""")

  def get_os_hourly_statistic(self, timestamp=None):
    if timestamp:
      return self.conn.execute(
          """SELECT * FROM os_family_hourly WHERE hour=?""", (timestamp,))
    return self.conn.execute(
        """SELECT * FROM os_family_hourly ORDER BY hour DESC""")

  def get_browser_hourly_statistic(self, timestamp=None):
    if timestamp:
      return self.conn.execute(
          """SELECT * FROM browser_family_hourly WHERE hour=?""", (timestamp,))
    return self.conn.execute(
        """SELECT * FROM browser_family_hourly ORDER BY hour DESC""")
