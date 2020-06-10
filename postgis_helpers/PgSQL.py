"""
Wrap up PostgreSQL

"""
import pandas as pd
import sqlalchemy
import psycopg2
from typing import Union

import geopandas as gpd
from geoalchemy2 import Geometry, WKTElement
import configparser
from pathlib import Path
import os
import urllib


class PostgreSQL():

    def __init__(self,
                 working_db: str,
                 un: str = "postgres",
                 pw: str = "password1",
                 host: str = "localhost",
                 port: int = 5432,
                 sslmode: Union[bool, str] = False,
                 super_db: str = "postgres",
                 super_un: str = "postgres",
                 super_pw: str = "password2",
                 verbosity: str = "full"):

        self.DATABASE = working_db
        self.USER = un
        self.PASSWORD = pw
        self.HOST = host
        self.PORT = port
        self.SSLMODE = sslmode
        self.SUPER_DB = super_db
        self.SUPER_USER = super_un
        self.SUPER_PASSWORD = super_pw

        self.VERBOSITY = verbosity

        _print(self, 1, f"Created an object for {self.DATABASE} @ {self.HOST}")

    def uri(self,
            super_uri: bool = False) -> str:

        if super_uri:
            user = self.SUPER_USER
            pw = self.SUPER_PASSWORD
            database = self.SUPER_DB
        else:
            user = self.USER
            pw = self.PASSWORD
            database = self.DATABASE

        connection_string = \
            f"postgresql://{user}:{pw}@{self.HOST}:{self.PORT}/{database}"

        if self.SSLMODE:
            connection_string += f"?sslmode={self.SSLMODE}"

        return connection_string

    def exists(self) -> bool:
        sql_db_exists = f"""
            SELECT EXISTS(
                SELECT datname FROM pg_catalog.pg_database
                WHERE lower(datname) = lower('{self.DATABASE}')
            );
        """
        return query_item(self, sql_db_exists, super_uri=True)

    def table_list(self):
        sql_all_tables = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """

        tables = query_list(self, sql_all_tables)

        table_names = [t[0] for t in tables]

        return table_names

    def create(self) -> None:
        if self.exists():
            _print(self, 1,
                   f"Database {self.DATABASE} already exists")
        else:
            _print(self, 3,
                   f"Creating database: {self.DATABASE} on {self.HOST}")

            sql_make_db = f"CREATE DATABASE {self.DATABASE};"

            execute(self, sql_make_db, autocommit=True)

            if "geometry_columns" in self.table_list():
                _print(self, 1, "PostGIS comes pre-installed")
            else:
                _print(self, 1, "Installing PostGIS")
                sql_add_postgis = "CREATE EXTENSION postgis;"
                execute(self, sql_add_postgis)

    def delete(self):
        if not self.exists():
            _print(self, 1, "This database does not exist, nothing to delete!")
        else:
            _print(self, 3, f"Deleting database! {self.DATABASE}")
            sql_drop_db = f"DROP DATABASE {self.DATABASE};"
            execute(self, sql_drop_db, autocommit=True)

    def add_uid_column(self, table_name):
        # Add a primary key column named 'uid'
        sql_unique_id_column = f"""
            ALTER TABLE {table_name} DROP COLUMN IF EXISTS uid;
            ALTER TABLE {table_name} ADD uid serial PRIMARY KEY;"""
        execute(self, sql_unique_id_column)

    def add_spatial_index(self, table_name):
        # Create a spatial index on the 'geom' column
        sql_make_spatial_index = f"""
            CREATE INDEX gix_{table_name}
            ON {table_name}
            USING GIST (geom);
        """
        execute(self, sql_make_spatial_index)

    def reproject_spatial_data(self,
                               table_name,
                               old_epsg,
                               new_epsg,
                               geom_type):

        sql_transform_geom = f"""
            ALTER TABLE {table_name}
            ALTER COLUMN geom TYPE geometry({geom_type}, {new_epsg})
            USING ST_Transform( ST_SetSRID( geom, {old_epsg} ), {new_epsg} );
        """
        execute(self, sql_transform_geom)

    def import_dataframe(self,
                         dataframe: pd.DataFrame,
                         table_name: str,
                         if_exists: str = "fail"):

        # Enforce clean column names!

        # Replace "Column Name" with "column_name"
        dataframe.columns = dataframe.columns.str.replace(' ', '_')
        dataframe.columns = [x.lower() for x in dataframe.columns]

        # Remove '.' and '-' from column names.
        # i.e. 'geo.display-label' becomes 'geodisplaylabel'
        for s in ['.', '-', '(', ')', '+']:
            dataframe.columns = dataframe.columns.str.replace(s, '')

        # Write to database
        engine = sqlalchemy.create_engine(self.uri())
        dataframe.to_sql(table_name, engine, if_exists=if_exists)
        engine.dispose()

        _print(self, 2, f"Imported dataframe to {table_name}")

    def import_geodataframe(self,
                            gdf: gpd.GeoDataFrame,
                            table_name: str,
                            src_epsg: Union[int, bool] = False):

        # Read the geometry type. It's possible there are
        # both MULTIPOLYGONS and POLYGONS. This grabs the MULTI variant

        geom_types = list(gdf.geometry.geom_type.unique())
        geom_typ = max(geom_types, key=len).upper()

        # Manually set the EPSG if the user passes one
        if src_epsg:
            gdf.crs = f"epsg:{src_epsg}"
            epsg_code = src_epsg

        # Otherwise, try to get the EPSG value directly from the geodataframe
        else:
            # Older gdfs have CRS stored as a dict: {'init': 'epsg:4326'}
            if type(gdf.crs) == dict:
                epsg_code = int(gdf.crs['init'].split(" ")[0].split(':')[1])
            # Now geopandas has a different approach
            else:
                epsg_code = int(str(gdf.crs).split(':')[1])

        # Sanitize the columns before writing to the database
        # Make all column names lower case
        gdf.columns = [x.lower() for x in gdf.columns]

        # Replace the 'geom' column with 'geometry'
        if 'geom' in gdf.columns:
            gdf['geometry'] = gdf['geom']
            gdf.drop('geom', 1, inplace=True)

        # Drop the 'gid' column
        if 'gid' in gdf.columns:
            gdf.drop('gid', 1, inplace=True)

        # Rename 'uid' to 'old_uid'
        if 'uid' in gdf.columns:
            gdf['old_uid'] = gdf['uid']
            gdf.drop('uid', 1, inplace=True)

        # Build a 'geom' column using geoalchemy2
        # and drop the source 'geometry' column
        gdf['geom'] = gdf['geometry'].apply(
                                    lambda x: WKTElement(x.wkt, srid=epsg_code)
        )
        gdf.drop('geometry', 1, inplace=True)

        # Write geodataframe to SQL database
        engine = sqlalchemy.create_engine(self.uri())
        gdf.to_sql(table_name, engine,
                   if_exists='replace', index=True, index_label='gid',
                   dtype={'geom': Geometry(geom_typ, srid=epsg_code)})
        engine.dispose()

        self.add_uid_column(table_name)
        self.add_spatial_index(table_name)

    def load_csv(self,
                 csv_path,
                 table_name,
                 if_exists: str = "append",
                 **csv_kwargs):
        # Read the CSV with whatever kwargs were passed
        df = pd.read_csv(csv_path, **csv_kwargs)

        self.import_dataframe(df, table_name, if_exists=if_exists)

    def load_geodata(self,
                     table_name,
                     data_path,
                     src_epsg: Union[int, bool] = False):

        _print(self, 2, f"Loading geodata into {table_name}")

        # Read the data into a geodataframe
        gdf = gpd.read_file(data_path)

        # Drop null geometries
        gdf = gdf[gdf['geometry'].notnull()]

        # Explode multipart to singlepart and reset the index
        gdf = gdf.explode()
        gdf['explode'] = gdf.index
        gdf = gdf.reset_index()

        self.import_geodataframe(gdf, table_name, src_epsg=src_epsg)

# ######### - ########## - ########## - ##########


def _print(db: PostgreSQL,
           level: int,
           message: str):
    """
        full = 1
        minimal = 2
        errors = 3


        1 = only prints in full
        2 = does not print in errors
        3 = always prints out
    """

    prefix = r"pGIS \\\ "

    msg = prefix + message

    if db.VERBOSITY == "full" and level in [1, 2, 3]:
        print(msg)

    elif db.VERBOSITY == "minimal" and level in [2, 3]:
        print(msg)

    elif db.VERBOSITY == "errors" and level in [3]:
        print(msg)


def query_df(db: PostgreSQL,
             query: str,
             super_uri: bool = False) -> pd.DataFrame:

    uri = db.uri(super_uri=super_uri)

    engine = sqlalchemy.create_engine(uri)
    df = pd.read_sql(query, engine)
    engine.dispose()

    return df


def query_geo_df(db: PostgreSQL,
                 query: str,
                 geom_col: str = "geom"):

    connection = psycopg2.connect(db.uri())

    gdf = gpd.GeoDataFrame.from_postgis(query, connection, geom_col=geom_col)

    connection.close()

    return gdf


def query_list(db: PostgreSQL,
               query: str,
               super_uri: bool = False) -> list:

    uri = db.uri(super_uri=super_uri)

    connection = psycopg2.connect(uri)
    cursor = connection.cursor()

    cursor.execute(query)
    result = cursor.fetchall()

    cursor.close()
    connection.close()

    return result


def query_item(db: PostgreSQL,
               query: str,
               super_uri: bool = False):

    # For when you want to transform [(True,)] into True
    result = query_list(db, query, super_uri=super_uri)

    return result[0][0]


def execute(db: PostgreSQL,
            query: str,
            autocommit: bool = False):

    uri = db.uri(super_uri=autocommit)

    connection = psycopg2.connect(uri)

    # Use autocommit when connecting to root db to make/delete other dbs
    if autocommit:
        connection.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
        )

    cursor = connection.cursor()

    cursor.execute(query)

    cursor.close()
    connection.commit()
    connection.close()


def get_config(working_db):

    config_file = os.path.join(Path.home(), ".postgis_helpers")

    print(config_file)
    if not os.path.exists(config_file):
        config_url = "https://raw.githubusercontent.com/aaronfraint/postGIS-tools/master/config-sample.txt"
        urllib.request.urlretrieve(config_url, config_file)

    # Parse the config.txt
    config_object = configparser.ConfigParser()
    config_object.read(config_file)

    connection_details = {}
    options = {}

    for host in config_object.sections():
        connection_details[host] = {key: config_object[host][key]
                                    for key in config_object[host]}
        options[host] = PostgreSQL(working_db, **connection_details[host])

    return options
