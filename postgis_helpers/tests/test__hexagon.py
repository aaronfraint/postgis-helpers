from ward import test, using

from postgis_helpers import PostgreSQL
from postgis_helpers.tests.fixtures import DataForTest, database_1, test_shp_data


# Can we make a hexagon layer covering another layer?
# ---------- ---------- ---------- ---------- ----------
def _test_hexagon_overlay(db: PostgreSQL, shp: DataForTest):

    hex_table_name = "test_hexagons"

    # Make a hexagon layer
    db.make_hexagon_overlay(
        hex_table_name, table_to_cover=shp.NAME, desired_epsg=2272, hexagon_size=5
    )

    assert hex_table_name in db.all_spatial_tables_as_dict()


@test("PostgreSQL().make_hexagon_overlay() creates a new spatial table")
@using(database=database_1, shp=test_shp_data)
def _(database, shp):
    _test_hexagon_overlay(database, shp)
