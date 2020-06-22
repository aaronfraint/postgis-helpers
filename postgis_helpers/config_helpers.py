""" You may have lots of SQL database connections.
Or maybe you just want to keep your database
connection information out of your analysis
code. This module provides a method for storing
and retrieving connections to database clusters
by named profiles inside ``[square_brackets]``.

"""
import os
import configparser
from pathlib import Path

DATA_ROOT = Path.home() / "sql_data_io"
DEFAULT_DATA_INBOX = DATA_ROOT / "inbox"
DEFAULT_DATA_OUTBOX = DATA_ROOT / "outbox"

starter_config_file = """
[DEFAULT]
pw = this-is-a-placeholder-password
port = 5432
super_db = postgres
super_un = postgres
super_pw = this-is-another-placeholder-password

[localhost]
host = localhost
un = postgres
pw = your-password-here

[digitalocean]
un = your-username-here
host = your-host-here.db.ondigitalocean.com
pw = your-password-here
port = 98765
sslmode = require
super_db = your_default_db
super_un = your_super_admin
super_pw = some_super_password12354
"""


def configurations(verbosity: str = "full") -> dict:

    config_file = os.path.join(Path.home(), ".postgis_helpers")

    print(f"Loading {config_file}")
    if not os.path.exists(config_file):
        print("Config file does not yet exist")
        print(f"Writing default config file to {config_file}")
        with open(config_file, "w") as open_file:
            open_file.write(starter_config_file)

    # Parse the config file saved to /Users/yourname/.postgis_helpers
    config = configparser.ConfigParser()
    config.read(config_file)

    return {host: config[host] for host in config.sections()}
