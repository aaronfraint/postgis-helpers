import pandas as pd
import geopandas as gpd


def spatialize_point_dataframe(
    df: pd.DataFrame, x_lon_col: str, y_lat_col: str, epsg: int
) -> gpd.GeoDataFrame:

    # Force lat and lon columns into float type
    df[y_lat_col] = df[y_lat_col].astype(float)
    df[x_lon_col] = df[x_lon_col].astype(float)

    point_geom = gpd.points_from_xy(df[x_lon_col], df[y_lat_col])
    gdf = gpd.GeoDataFrame(df, geometry=point_geom)
    gdf.crs = f"EPSG:{epsg}"

    return gdf


def get_centroid_xy_of_gdf(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    x = gdf.geometry.unary_union.centroid.x
    y = gdf.geometry.unary_union.centroid.y

    return [y, x]
