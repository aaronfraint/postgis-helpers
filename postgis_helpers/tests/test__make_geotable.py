from ward import test, using

from postgis_helpers import PostgreSQL
from postgis_helpers.tests.fixtures import DataForTest, database_1, test_shp_data


# Can we make new geometric tables from queries?
# ---------- ---------- ---------- ---------- --
def _test_make_geotable_from_query(db: PostgreSQL,
                                   shp: DataForTest):

    new_geotable = "test_make_geotable_multilinestring"

    query = f"""
        SELECT ST_UNION(geom) AS geom
        FROM {shp.NAME}
    """

    # Make a new geotable
    db.make_geotable_from_query(query,
                                new_geotable,
                                geom_type="MULTILINESTRING",
                                epsg=shp.EPSG)

    # Confirm that the new table's EPSG matches the expected value
    epsg = db.all_spatial_tables_as_dict()[new_geotable]

    assert epsg == shp.EPSG


@test("PostgreSQL().make_geotable_from_query() creates a geo table with proper EPSG")
@using(database=database_1, shp=test_shp_data)
def _(database, shp):
    _test_make_geotable_from_query(database, shp)
