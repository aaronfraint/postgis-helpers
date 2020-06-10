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

    def _print(self,
               level: int,
               message: str):
        """ Messages will print out depending on the VERBOSITY property
            and the importance level provided.

            VERBOSITY options include: ``full``, ``minimal``, and ``errors``

            1 = Only prints in ``full``
            2 = Prints in ``full`` and ``minimal``,
                but does not print in ``errors``
            3 = Always prints out


        :param level: [description]
        :type level: int
        :param message: [description]
        :type message: str
        """

        prefix = r"\\ pGIS \\ "

        msg = prefix + message

        if self.VERBOSITY == "full" and level in [1, 2, 3]:
            print(msg)

        elif self.VERBOSITY == "minimal" and level in [2, 3]:
            print(msg)

        elif self.VERBOSITY == "errors" and level in [3]:
            print(msg)

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
        """Initialize a database object with placeholder values.

        :param working_db: Name of the database you want to connect to
        :type working_db: str
        :param un: User name within the database, defaults to "postgres"
        :type un: str, optional
        :param pw: Password for the user, defaults to "password1"
        :type pw: str, optional
        :param host: Host where the database lives, defaults to "localhost"
        :type host: str, optional
        :param port: Port number on the host, defaults to 5432
        :type port: int, optional
        :param sslmode: False or string like "require", defaults to False
        :type sslmode: Union[bool, str], optional
        :param super_db: SQL cluster root db, defaults to "postgres"
        :type super_db: str, optional
        :param super_un: SQL cluster root user, defaults to "postgres"
        :type super_un: str, optional
        :param super_pw: SQL cluster root password, defaults to "password2"
        :type super_pw: str, optional
        :param verbosity: Control how much gets printed out to the console,
                          defaults to "full". Other options include "minimal"
                          and "errors"
        :type verbosity: str, optional
        """

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

        self._print(1, f"Created an object for {self.DATABASE} @ {self.HOST}")

    # Query the database
    # ------------------

    def query_as_list(self,
                      query: str,
                      super_uri: bool = False) -> list:

        uri = self.uri(super_uri=super_uri)

        connection = psycopg2.connect(uri)

        cursor = connection.cursor()

        cursor.execute(query)

        result = cursor.fetchall()

        cursor.close()
        connection.close()

        return result

    def query_as_df(self,
                    query: str,
                    super_uri: bool = False) -> pd.DataFrame:

        uri = self.uri(super_uri=super_uri)

        engine = sqlalchemy.create_engine(uri)
        df = pd.read_sql(query, engine)
        engine.dispose()

        return df

    def query_as_geo_df(self,
                        query: str,
                        geom_col: str = "geom") -> gpd.GeoDataFrame:

        connection = psycopg2.connect(self.uri())

        gdf = gpd.GeoDataFrame.from_postgis(query, connection,
                                            geom_col=geom_col)

        connection.close()

        return gdf

    def query_single_item(self,
                          query: str,
                          super_uri: bool = False):

        # For when you want to transform [(True,)] into True
        result = self.query_as_list(query, super_uri=super_uri)

        return result[0][0]

    def execute(self,
                query: str,
                autocommit: bool = False):

        uri = self.uri(super_uri=autocommit)

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

    # Database-level helper functions
    # -------------------------------

    def uri(self, super_uri: bool = False) -> str:
        """Create a connection string URI for this database.

        :param super_uri: Flag that will provide access to cluster
                          root if True, defaults to False
        :type super_uri: bool, optional
        :return: Connection string URI for PostgreSQL
        :rtype: str
        """

        # If super_uri is True, use the super un/pw/db
        if super_uri:
            user = self.SUPER_USER
            pw = self.SUPER_PASSWORD
            database = self.SUPER_DB

        # Otherwise, use the normal connection info
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
        """Does this database exist yet? True or False

        :return: True or False if the database exists on the cluster
        :rtype: bool
        """

        sql_db_exists = f"""
            SELECT EXISTS(
                SELECT datname FROM pg_catalog.pg_database
                WHERE lower(datname) = lower('{self.DATABASE}')
            );
        """
        return self.query_single_item(sql_db_exists, super_uri=True)

    def table_list(self) -> list:
        """Get a list of all tables in the database

        :return: List of tables in the database
        :rtype: list
        """

        sql_all_tables = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """

        tables = self.query_as_list(sql_all_tables)

        table_names = [t[0] for t in tables]

        return table_names

    def column_list(self, table_name: str) -> list:
        pass

    def create(self) -> None:
        """Create this database (if it doesn't exist yet)"""

        if self.exists():
            self._print(1, f"Database {self.DATABASE} already exists")
        else:
            self._print(3,
                        f"Creating database: {self.DATABASE} on {self.HOST}")

            sql_make_db = f"CREATE DATABASE {self.DATABASE};"

            self.execute(sql_make_db, autocommit=True)

            if "geometry_columns" in self.table_list():
                self._print(1, "PostGIS comes pre-installed")
            else:
                self._print(1, "Installing PostGIS")

                sql_add_postgis = "CREATE EXTENSION postgis;"
                self.execute(sql_add_postgis)

    def delete(self) -> None:
        """Delete this database (if it exists)"""

        if not self.exists():
            self._print(1, "This database does not exist, nothing to delete!")
        else:
            self._print(3, f"Deleting database! {self.DATABASE}")
            sql_drop_db = f"DROP DATABASE {self.DATABASE};"
            self.execute(sql_drop_db, autocommit=True)

    def add_uid_column(self, table_name: str) -> None:
        """Add a serial primary key column named 'uid' to the table.

        :param table_name: Name of the table to add a uid column to
        :type table_name: str
        """

        sql_unique_id_column = f"""
            ALTER TABLE {table_name} DROP COLUMN IF EXISTS uid;
            ALTER TABLE {table_name} ADD uid serial PRIMARY KEY;"""
        self.execute(sql_unique_id_column)

    def add_spatial_index(self, table_name: str) -> None:
        """Add a spatial index to the 'geom' column in the table.

        :param table_name: Name of the table to make the index on
        :type table_name: str
        """

        sql_make_spatial_index = f"""
            CREATE INDEX gix_{table_name}
            ON {table_name}
            USING GIST (geom);
        """
        self.execute(sql_make_spatial_index)

    def reproject_spatial_data(self,
                               table_name: str,
                               old_epsg: Union[int, str],
                               new_epsg: Union[int, str],
                               geom_type: str) -> None:
        """Transform spatial data from one EPSG into another EPSG.

        :param table_name: name of the table
        :type table_name: str
        :param old_epsg: Current EPSG of the data
        :type old_epsg: Union[int, str]
        :param new_epsg: Desired new EPSG for the data
        :type new_epsg: Union[int, str]
        :param geom_type: PostGIS-valid name of the
                          geometry you're transforming
        :type geom_type: str
        """

        sql_transform_geom = f"""
            ALTER TABLE {table_name}
            ALTER COLUMN geom TYPE geometry({geom_type}, {new_epsg})
            USING ST_Transform( ST_SetSRID( geom, {old_epsg} ), {new_epsg} );
        """
        self.execute(sql_transform_geom)

    def import_dataframe(self,
                         dataframe: pd.DataFrame,
                         table_name: str,
                         if_exists: str = "fail") -> None:
        """Import an in-memory Pandas dataframe to the SQL database.

        Enforce clean column names (without spaces, caps, or weird symbols).

        :param dataframe: dataframe with data you want to save
        :type dataframe: pd.DataFrame
        :param table_name: name of the table that will get created
        :type table_name: str
        :param if_exists: pandas argument to handle overwriting data,
                          defaults to "fail"
        :type if_exists: str, optional
        """

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

        self._print(2, f"Imported dataframe to {table_name}")

    def import_geodataframe(self,
                            gdf: gpd.GeoDataFrame,
                            table_name: str,
                            src_epsg: Union[int, bool] = False):
        """Import an in-memory Geopands geodataframe to the SQL database.

        :param gdf: geodataframe with data you want to save
        :type gdf: gpd.GeoDataFrame
        :param table_name: name of the table that will get created
        :type table_name: str
        :param src_epsg: The source EPSG code can be passed as an integer.
                         By default this function will try to read the EPSG
                         code directly, but some spatial data is funky and
                         requires that you explicitly declare its projection.
                         Defaults to False
        :type src_epsg: Union[int, bool], optional
        """

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
                 table_name: str,
                 csv_path: Union[Path, str],
                 if_exists: str = "append",
                 **csv_kwargs):
        r"""Load a CSV into a dataframe, then save the df to SQL.

        :param table_name: Name of the table you want to create
        :type table_name: str
        :param csv_path: Path to data. Anything accepted by Pandas works here.
        :type csv_path: Union[Path, str]
        :param if_exists: How to handle overwriting existing data,
                          defaults to "append"
        :type if_exists: str, optional
        :param \**csv_kwargs: any kwargs for ``pd.read_csv()`` are valid here.
        """

        # Read the CSV with whatever kwargs were passed
        df = pd.read_csv(csv_path, **csv_kwargs)

        self.import_dataframe(df, table_name, if_exists=if_exists)

    def load_geodata(self,
                     table_name: str,
                     data_path: Union[Path, str],
                     src_epsg: Union[int, bool] = False):
        """Load geographic data into a geodataframe, then save to SQL.

        :param table_name: Name of the table you want to create
        :type table_name: str
        :param data_path: Path to the data. Anything accepted by Geopandas
                          works here.
        :type data_path: Union[Path, str]
        :param src_epsg: Manually declare the source EPSG if needed,
                         defaults to False
        :type src_epsg: Union[int, bool], optional
        """

        self._print(2, f"Loading geodata into {table_name}")

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


starter_config_file = """
[DEFAULT]
pw = this-is-a-placeholder-password
port = 5432
super_db = postgres
super_un = postgres
super_pw = this-is-another-placeholder-password

[localhost]
host = localhost
un = postgres
pw = your-password-here

[digitalocean]
un = your-username-here
host = your-host-here.db.ondigitalocean.com
pw = your-password-here
port = 98765
sslmode = require
super_db = your_default_db
super_un = your_super_admin
super_pw = some_super_password12354
"""


def get_config(working_db: str,
               verbosity: str = "minimal") -> dict:

    config_file = os.path.join(Path.home(), ".postgis_helpers")

    print(config_file)
    if not os.path.exists(config_file):
        print(f"Writing default config file to {config_file}")
        with open(config_file, "w") as open_file:
            open_file.write(starter_config_file)
        # config_url = "https://raw.githubusercontent.com/aaronfraint/postGIS-tools/master/config-sample.txt"
        # urllib.request.urlretrieve(config_url, config_file)

    # Parse the config.txt
    config = configparser.ConfigParser()
    config.read(config_file)

    return {host: PostgreSQL(working_db,
                             verbosity=verbosity,
                             **config[host])
            for host in config.sections()}
