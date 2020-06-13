from ward import test, using

from postgis_helpers import PostgreSQL
from postgis_helpers.tests.fixtures import database_local, database_remote, CSV_NAME, SHP_NAME


# Can we transfer tabular data between DBs/Hosts?
# ---------- ---------- ---------- ---------- ----------
def _test_transfer_data_tabular(db1: PostgreSQL,
                                db2: PostgreSQL,
                                table_name: str):

    # Make sure that this table does not exist in the 2nd database
    if table_name in db2.all_tables_as_list():
        db2.table_delete(table_name)

    # Transfer to the 2nd database
    db1.transfer_data_to_another_db(table_name, db2)

    # Confirm there is now a table in the DB
    assert table_name in db2.all_tables_as_list()


@test("PostgreSQL().transfer_data_to_another_db() works with tabular data")
@using(database1=database_local,
       database2=database_remote,
       table_name=CSV_NAME)
def _(database1, database2, table_name):
    _test_transfer_data_tabular(database1, database2, table_name)


# Can we transfer spatial data between DBs/Hosts?
# ---------- ---------- ---------- ---------- ----------
def _test_transfer_data_spatial(db1: PostgreSQL, 
                                db2: PostgreSQL,
                                table_name: str):

    # Make sure that this table does not exist in the 2nd database
    if table_name in db2.all_tables_as_list():
        db2.table_delete(table_name)

    # Transfer to the 2nd database
    db1.transfer_data_to_another_db(table_name, db2)

    # Confirm there is now a table in the DB
    assert table_name in db2.all_spatial_tables_as_dict()


@test("PostgreSQL().transfer_data_to_another_db() works with spatial data")
@using(database1=database_local,
       database2=database_remote,
       table_name=SHP_NAME)
def _(database1, database2, table_name):
    _test_transfer_data_spatial(database1, database2, table_name)
