import pandas as pd
from ward import test, using

from postgis_helpers import PostgreSQL
from postgis_helpers.tests.fixtures import DataForTest, database_local, test_csv_data


# Does the table exist after importing it?
# ---------- ---------- ---------- -------
def _test_import_csv(db: PostgreSQL,
                     csv: DataForTest):

    # Confirm the CSV is now a table in the DB
    assert csv.NAME in db.all_tables_as_list()


@test("PostgreSQL().import_csv() imports a table")
@using(db=database_local, csv=test_csv_data)
def _(db, csv):
    _test_import_csv(db, csv)


# Does the SQL table have the same number of rows as the dataframe?
# ---------- ---------- ---------- ---------- ---------- ----------
def _test_import_csv_matches(db: PostgreSQL,
                             csv: DataForTest):

    # Import a CSV file
    df = pd.read_csv(csv.PATH)

    # Get the number of rows in the raw dataframe
    csv_row_count, _ = df.shape

    # Get the number of rows in the new SQL table
    query = f"""
        SELECT COUNT(*) FROM {csv.NAME}
    """
    db_table_row_count = db.query_as_single_item(query)

    # Confirm that the dataframe & SQL table have matching rowcounts
    assert csv_row_count == db_table_row_count


@test("PostgreSQL().import_csv() imports a table with all rows")
@using(db=database_local, csv=test_csv_data)
def _(db, csv):
    _test_import_csv_matches(db, csv)
