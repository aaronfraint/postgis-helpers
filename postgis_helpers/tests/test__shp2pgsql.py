import geopandas as gpd

from ward import test, using

from postgis_helpers import PostgreSQL
from postgis_helpers.tests.fixtures import DataForTest, database_1, test_shp_data


# Does the wrapper around shp2pgsql properly import a .shp file?
# ---------- ---------- ---------- ---------- --------- -------
def _test_shp2pgsql(db: PostgreSQL,
                    shp: DataForTest):

    # Import the shapefile
    _ = db.shp2pgsql(shp.NAME, shp.IMPORT_FILEPATH)

    assert shp.NAME in db.all_spatial_tables_as_dict()


@test("PostgreSQL().shp2pgsql() creates a new spatial SQL table")
@using(database=database_1, shp=test_shp_data)
def _(database, shp):
    _test_shp2pgsql(database, shp)


# Does the new SQL table have a matching EPSG to the .shp?
# ---------- ---------- ---------- ---------- ------------
def _test_shp2pgsql_epsg(db: PostgreSQL,
                         shp: DataForTest):

    # Import the shapefile
    _ = db.shp2pgsql(shp.NAME, shp.IMPORT_FILEPATH)

    # Confirm SQL table and shapefile both have same EPSGs
    gdf = gpd.read_file(shp.IMPORT_FILEPATH)
    shapefile_crs = gdf.crs

    sql_epsg = db.all_spatial_tables_as_dict()[shp.NAME]

    assert sql_epsg == shapefile_crs


@test("PostgreSQL().shp2pgsql() result in SQL has EPSG that matches the .shp")
@using(database=database_1, shp=test_shp_data)
def _(database, shp):
    _test_shp2pgsql_epsg(database, shp)
