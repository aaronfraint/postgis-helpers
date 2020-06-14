from ward import test, using, each

from postgis_helpers import PostgreSQL
from postgis_helpers.tests.fixtures import database_1


# Does the database still exist after deleting it?
# ---------- ---------- ---------- ---------- ----
def _test_db_delete(db: PostgreSQL):

    db.db_delete()

    assert not db.exists()


@test('PostgreSQL().delete() has removed "{database.DATABASE}" from localhost')
@using(database=database_1)
def _(database):
    _test_db_delete(database)
