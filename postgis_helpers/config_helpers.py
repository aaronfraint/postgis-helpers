""" 
Summary of ``config_helpers.py``
--------------------------------

You may have lots of SQL database connections.
Or maybe you just want to keep your database
connection information out of your analysis
code. This module provides a method for storing
and retrieving connections to database clusters
by named profiles inside ``[square_brackets]``.

TODO: update this with new features
"""
import configparser
from pathlib import Path
from typing import Union

from .console import _console

DATA_ROOT = Path.home() / "sql_data_io"
DEFAULT_DATA_INBOX = DATA_ROOT / "inbox"
DEFAULT_DATA_OUTBOX = DATA_ROOT / "outbox"

DB_CONFIG_FILEPATH = DATA_ROOT / "database_connections.cfg"

STARTER_CONFIG_FILE = """
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


def make_config_file(filepath: Union[Path, str] = DB_CONFIG_FILEPATH,
                     overwrite: bool = False) -> bool:

    filepath = Path(filepath)

    # Make sure the parent fiolder exists
    if not filepath.parent.exists():
        filepath.parent.mkdir(parents=True)

    if not overwrite and filepath.exists():
        _console.print(f":eyes: Config file already exists and overwrite=False. Will not overwrite.")
        return False

    else:
        if filepath.exists():
            _console.print(f":fast-forward_button: Overwriting config file at {filepath}")
        else:
            _console.print(f":play_button: Creating a new config file at {filepath}")

        with open(filepath, "w") as open_file:
            open_file.write(STARTER_CONFIG_FILE)

        return True


def read_config_file(filepath: Path = DB_CONFIG_FILEPATH) -> dict:

    config = configparser.ConfigParser()
    config.read(filepath)

    all_hosts = {}

    for host in config.sections():
        all_hosts[host] = {}

        for key in config[host]:
            value = config[host][key]

            all_hosts[host][key] = value

    _console.print(f"Loaded db configurations from {filepath}")

    return all_hosts


def configurations(filepath: Path = DB_CONFIG_FILEPATH) -> dict:

    if not filepath.exists():
        make_config_file(filepath)

    return read_config_file(filepath)
