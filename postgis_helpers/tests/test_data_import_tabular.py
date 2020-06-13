import pandas as pd
from ward import test, using

from postgis_helpers import PostgreSQL
from postgis_helpers.tests.fixtures import database_local, CSV_NAME, CSV_URL


# Does the table exist after importing it?
# ---------- ---------- ---------- -------
def _test_import_csv(db: PostgreSQL,
                     table_name: str):

    # Confirm the CSV is now a table in the DB
    assert table_name in db.all_tables_as_list()


@test("PostgreSQL().import_csv() imports a table")
@using(db=database_local, table_name=CSV_NAME)
def _(db, table_name):
    _test_import_csv(db, table_name)


# Does the SQL table have the same number of rows as the dataframe?
# ---------- ---------- ---------- ---------- ---------- ----------
def _test_import_csv_matches(db: PostgreSQL,
                             table_name: str,
                             file_path: str):

    # Import a CSV file
    df = pd.read_csv(file_path)

    # Get the number of rows in the raw dataframe
    csv_row_count, _ = df.shape

    # Get the number of rows in the new SQL table
    query = f"""
        SELECT COUNT(*) FROM {table_name}
    """
    db_table_row_count = db.query_as_single_item(query)

    # Confirm that the dataframe & SQL table have matching rowcounts
    assert csv_row_count == db_table_row_count


@test("PostgreSQL().import_csv() imports a table with all rows")
@using(db=database_local, table_name=CSV_NAME, file_path=CSV_URL)
def _(db, table_name, file_path):
    _test_import_csv_matches(db, table_name, file_path)
