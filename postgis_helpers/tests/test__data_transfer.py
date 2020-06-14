from ward import test, using

from postgis_helpers import PostgreSQL
from postgis_helpers.tests.fixtures import DataForTest, database_1, database_2, test_csv_data, test_shp_data


# Can we transfer tabular data between DBs/Hosts?
# ---------- ---------- ---------- ---------- ----------
def _test_transfer_data_tabular(db1: PostgreSQL,
                                db2: PostgreSQL,
                                csv: DataForTest):

    table_name = csv.NAME

    # Make sure that this table does not exist in the 2nd database
    if table_name in db2.all_tables_as_list():
        db2.table_delete(table_name)

    # Transfer to the 2nd database
    db1.transfer_data_to_another_db(table_name, db2)

    # Confirm there is now a table in the DB
    assert table_name in db2.all_tables_as_list()


@test("PostgreSQL().transfer_data_to_another_db() works with tabular data")
@using(database1=database_1,
       database2=database_2,
       csv=test_csv_data)
def _(database1, database2, csv):
    _test_transfer_data_tabular(database1, database2, csv)


# Can we transfer spatial data between DBs/Hosts?
# ---------- ---------- ---------- ---------- ----------
def _test_transfer_data_spatial(db1: PostgreSQL,
                                db2: PostgreSQL,
                                shp: DataForTest):

    table_name = shp.NAME

    # Make sure that this table does not exist in the 2nd database
    if table_name in db2.all_tables_as_list():
        db2.table_delete(table_name)

    # Transfer to the 2nd database
    db1.transfer_data_to_another_db(table_name, db2)

    # Confirm there is now a table in the DB
    assert table_name in db2.all_spatial_tables_as_dict()


@test("PostgreSQL().transfer_data_to_another_db() works with spatial data")
@using(database1=database_1,
       database2=database_2,
       shp=test_shp_data)
def _(database1, database2, shp):
    _test_transfer_data_spatial(database1, database2, shp)
