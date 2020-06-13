from ward import test, using, each

from postgis_helpers import PostgreSQL
from postgis_helpers.tests.fixtures import database_local


# Does the database still exist after deleting it?
# ---------- ---------- ---------- ---------- ----
def _test_db_delete(db: PostgreSQL):

    db.delete()

    assert not db.exists()


@test('PostgreSQL().delete() has removed "{database.DATABASE}" from localhost')
@using(database=database_local)
def _(database):
    _test_db_delete(database)
