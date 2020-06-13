from ward import test, using

from postgis_helpers import PostgreSQL
from postgis_helpers.tests.fixtures import database_local, SHP_NAME


# Can we make a hexagon layer covering another layer?
# ---------- ---------- ---------- ---------- ----------
def _test_hexagon_overlay(db: PostgreSQL,
                          table_name: str):

    # Make a hexagon layer
    db.make_hexagon_overlay("test_hexagons",
                            table_to_cover=table_name,
                            desired_epsg=2272,
                            hexagon_size=5)

    assert "test_hexagons" in db.all_spatial_tables_as_dict()


@test("PostgreSQL().transfer_data_to_another_db() works with tabular data")
@using(database=database_local, table_name=SHP_NAME)
def _(database, table_name):
    _test_hexagon_overlay(database, table_name)
