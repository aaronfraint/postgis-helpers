"""
This module defines local and remote database fixtures.
These fixtures are used in the tests contained in the
``test_*.py`` files.

TODO: replace the ``digitalocean`` reference with
any non-localhost key present.
"""
from ward import fixture

from typing import Union
from pathlib import Path

from postgis_helpers import PostgreSQL, configurations


class DataForTest():

    def __init__(self,
                 table_name: str,
                 file_path: Union[Path, str],
                 epsg: Union[bool, int] = False):
        self.NAME = table_name
        self.PATH = file_path
        self.EPSG = epsg

    def is_spatial(self):
        if self.EPSG:
            return True
        else:
            return False


test_csv_data = DataForTest(
    "covid_2020_06_10",
    "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports_us/06-10-2020.csv"
)

test_shp_data = DataForTest(
    "philly_vz_hin_2017",
    "https://phl.carto.com/api/v2/sql?q=SELECT+*+FROM+high_injury_network_2017&filename=high_injury_network_2017&format=shp&skipfields=cartodb_id",
    2272
)


@fixture(scope="global")
def database_local():

    # Set up the database
    db = PostgreSQL("test_from_ward",
                    verbosity="minimal",
                    **configurations()["localhost"])
    db.create()

    # Import CSV and shapefile data sources
    db.import_csv(test_csv_data.NAME, test_csv_data.PATH)
    db.import_geodata(test_shp_data.NAME, test_shp_data.PATH, if_exists="replace")

    # Yield to the test
    yield db

    # Don't tear down the database on the local server!!!
    # This is done later as part of test_final_cleanup.py


@fixture(scope="global")
def database_remote():

    # Set up the database
    db = PostgreSQL("test_from_ward",
                    verbosity="minimal",
                    **configurations()["digitalocean"])
    db.create()

    # Yield to the test
    yield db

    # Tear down the database on the remote server
    db.delete()
