from ward import test, using

from postgis_helpers import PostgreSQL
from postgis_helpers.tests.fixtures import database_1


# Does the .sql file exist after running pg_dump?
# ---------- ---------- ---------- ---------- ---
def _test_db_export_pgdump_file(db: PostgreSQL):

    # Export a spatial table to shapefile
    output_sql_file = db.db_export_pgdump_file(db.DATA_OUTBOX)

    assert output_sql_file.exists()


@test("PostgreSQL().db_export_pgdump_file() creates a .SQL file")
@using(database=database_1)
def _(database):
    _test_db_export_pgdump_file(database)
