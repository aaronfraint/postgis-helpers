from ward import fixture

from postgis_helpers import PostgreSQL, configurations

CSV_NAME = "covid_2020_06_10"
CSV_URL = "\
https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master\
/csse_covid_19_data/csse_covid_19_daily_reports_us/06-10-2020.csv"

SHP_NAME = "philly_vz_hin_2017"
SHP_URL = "\
https://phl.carto.com/api/v2\
/sql?q=SELECT+*+FROM+high_injury_network_2017\
&filename=high_injury_network_2017\
&format=shp&skipfields=cartodb_id"


@fixture(scope="global")
def database_local():

    # Set up the database
    db = PostgreSQL("test_from_ward",
                    verbosity="full",
                    **configurations()["localhost"])
    db.create()

    # Import CSV and shapefile data sources
    db.import_csv(CSV_NAME, CSV_URL)
    db.import_geodata(SHP_NAME, SHP_URL, if_exists="replace")

    # Yield to the test
    yield db

    # Tear down the database
    db.delete()


@fixture(scope="global")
def database_remote():

    # Set up the database
    db = PostgreSQL("test_from_ward",
                    verbosity="minimal",
                    **configurations()["digitalocean"])
    db.create()

    # Yield to the test
    yield db

    # Tear down the database
    db.delete()
