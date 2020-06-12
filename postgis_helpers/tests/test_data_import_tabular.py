from ward import test, using

from postgis_helpers import PostgreSQL
from postgis_helpers.tests.fixtures import database_local

CSV_URL = "https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv?raw=true"


# Does the table exist after importing it?
# ---------- ---------- ---------- -------
def _test_import_csv(db: PostgreSQL,
                     table_name: str,
                     file_path: str = CSV_URL):

    # Import a CSV file
    db.import_csv(table_name, file_path, if_exists="replace")

    # Confirm there is now a table in the DB
    assert table_name in db.all_tables_as_list()


@test("PostgreSQL().import_csv() imports a table")
@using(db=database_local)
def _(db):
    _test_import_csv(db, "covid_confirmed_us", CSV_URL)


# Does the SQL table have the same number of rows as the dataframe?
# ---------- ---------- ---------- ---------- ---------- ----------
def _test_import_csv_matches(db: PostgreSQL,
                             table_name: str,
                             file_path: str = CSV_URL):

    # Import a CSV file
    df = db.import_csv(table_name, file_path, if_exists="replace")

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
@using(db=database_local)
def _(db):
    _test_import_csv(db, "covid_confirmed_us", CSV_URL)
