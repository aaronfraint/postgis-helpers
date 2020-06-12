from ward import test, using

from postgis_helpers import PostgreSQL
from postgis_helpers.tests.fixtures import database_local, database_remote


CSV_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports_us/06-10-2020.csv"
SHP_URL = "https://phl.carto.com/api/v2/sql?q=SELECT+*+FROM+high_injury_network_2017&filename=high_injury_network_2017&format=shp&skipfields=cartodb_id"


# Can we transfer tabular data between DBs/Hosts?
# ---------- ---------- ---------- ---------- ----------
def _test_transfer_data_tabular(db1: PostgreSQL,
                                db2: PostgreSQL):

    table_name = "covid_us_2020_06_10"

    # Make sure that this table does not exist in the 2nd database
    if table_name in db2.all_tables_as_list():
        db2.table_delete(table_name)

    # Import a CSV file into the 1st database
    db1.import_csv(table_name, CSV_URL, if_exists="replace")

    # Transfer to the 2nd database
    db1.transfer_data_to_another_db(table_name, db2)

    # Confirm there is now a table in the DB
    assert table_name in db2.all_tables_as_list()


@test("PostgreSQL().transfer_data_to_another_db() works with tabular data")
@using(database1=database_local,
       database2=database_remote)
def _(database1, database2):
    _test_transfer_data_tabular(database1, database2)


# Can we transfer spatial data between DBs/Hosts?
# ---------- ---------- ---------- ---------- ----------
def _test_transfer_data_spatial(db1, db2):

    table_name = "philly_vz_hin_2017"

    # Make sure that this table does not exist in the 2nd database
    if table_name in db2.all_tables_as_list():
        db2.table_delete(table_name)

    # Import a shapefile into the 1st database
    db1.import_geodata(table_name, SHP_URL, if_exists="replace")

    # Transfer to the 2nd database
    db1.transfer_data_to_another_db(table_name, db2)

    # Confirm there is now a table in the DB
    assert table_name in db2.all_spatial_tables_as_dict()


@test("PostgreSQL().transfer_data_to_another_db() works with spatial data")
@using(database1=database_local,
       database2=database_remote)
def _(database1, database2):
    _test_transfer_data_spatial(database1, database2)
