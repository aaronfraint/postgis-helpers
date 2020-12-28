import geopandas as gpd

from ward import test, using

from postgis_helpers import PostgreSQL
from postgis_helpers.tests.fixtures import DataForTest, database_1, test_shp_data


# Does the wrapper around pgsql2shp produce a new .shp file?
# ---------- ---------- ---------- ---------- --------- ----
def _test_pgsql2shp(db: PostgreSQL, shp: DataForTest):

    # Export a spatial table to shapefile
    output_shp = db.pgsql2shp(shp.NAME, shp.EXPORT_FOLDER)

    assert output_shp.exists()


@test("PostgreSQL().pgsql2shp() creates a new shapefile")
@using(database=database_1, shp=test_shp_data)
def _(database, shp):
    _test_pgsql2shp(database, shp)


# Does the new .shp file have a matching EPSG?
# ---------- ---------- ---------- ----------
def _test_pgsql2shp_epsg(db: PostgreSQL, shp: DataForTest):

    # Export a spatial table to shapefile
    output_shp = db.pgsql2shp(shp.NAME, shp.EXPORT_FOLDER)

    gdf = gpd.read_file(output_shp)

    sql_epsg = db.all_spatial_tables_as_dict()[shp.NAME]

    shapefile_crs = gdf.crs

    assert sql_epsg == shapefile_crs


@test("PostgreSQL().pgsql2shp() output has EPSG that matches the SQL table")
@using(database=database_1, shp=test_shp_data)
def _(database, shp):
    _test_pgsql2shp_epsg(database, shp)
