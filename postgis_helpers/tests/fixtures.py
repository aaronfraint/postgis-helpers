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


class DataForTest:
    def __init__(
        self,
        table_name: str,
        data_type: str,
        url_path: str = None,
        epsg: int = None,
        import_folder: Path = None,
        export_folder: Path = None,
    ):

        self.NAME = table_name
        self.PATH_URL = url_path
        self.EPSG = epsg
        self.DATA_TYPE = data_type

        # FOLDER TO SAVE EXPORTED DATA
        if not export_folder:
            self.EXPORT_FOLDER = Path.home() / "postgis_helpers" / "test_data"
        else:
            self.EXPORT_FOLDER = export_folder

        # FOLDER TO LOAD DOWNLOADED DATA FROM
        if not import_folder:
            self.IMPORT_FOLDER = Path.home() / "Downloads" / table_name
        else:
            self.IMPORT_FOLDER = import_folder

        # Ensure that these folders exist
        if not self.EXPORT_FOLDER.exists():
            self.EXPORT_FOLDER.mkdir(parents=True)

        if not self.IMPORT_FOLDER.exists():
            self.IMPORT_FOLDER.mkdir(parents=True)

        self.IMPORT_FILEPATH = self.IMPORT_FOLDER / f"{self.NAME}.{self.DATA_TYPE}"

    def is_spatial(self):
        if self.EPSG:
            return True
        else:
            return False

    def flush_local_data(self):
        data_folder = self.EXPORT_FOLDER / self.NAME
        for f in data_folder.iterdir():
            if f.is_file():
                f.unlink()
        data_folder.rmdir()
        print(f"Deleted {data_folder}")


test_csv_data = DataForTest(
    "covid_2020_06_10",
    "csv",
    "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports_us/06-10-2020.csv",
)

test_shp_data = DataForTest(
    "high_injury_network_2017",
    "shp",
    "https://phl.carto.com/api/v2/sql?q=SELECT+*+FROM+high_injury_network_2017&filename=high_injury_network_2017&format=shp&skipfields=cartodb_id",
    2272,
)


@fixture(scope="global")
def database_1():

    # Set up the database
    db = PostgreSQL(
        "test_from_ward", verbosity="minimal", **configurations()["localhost"]
    )

    # Import CSV and shapefile data sources
    db.import_csv(test_csv_data.NAME, test_csv_data.PATH_URL, if_exists="replace")
    db.import_geodata(test_shp_data.NAME, test_shp_data.PATH_URL, if_exists="replace")

    # Yield to the test
    yield db

    # Don't tear down this database!!!
    # This is done later as part of test_final_cleanup.py

    # Delete temp shapefiles
    test_shp_data.flush_local_data()
    test_csv_data.flush_local_data()


@fixture(scope="global")
def database_2():

    # Set up the database
    db = PostgreSQL(
        "test_from_ward_2", verbosity="minimal", **configurations()["localhost"]
    )

    # Yield to the test
    yield db

    # Tear down this database automatically
    db.db_delete()
