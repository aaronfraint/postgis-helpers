import click
from pathlib import Path

from postgis_helpers.PgSQL import PostgreSQL
from postgis_helpers.config_helpers import (
    configurations,
    make_config_file,
    DB_CONFIG_FILEPATH
)
from postgis_helpers.console import _console, RichProgress


@click.group()
def main():
    """ pGIS is a command-line utility for interacting
    with the postgis_helpers Python package.

    To get more information on a particular command,
    type: pGIS COMMAND --help

    All available commands are shown below.
    """
    pass


# CREATE A CONFIGURATION FILE
# ---------------------------


@main.command()
@click.option(
    "--filepath", "-f",
    help="Custom filepath for a new configuration file",
    default=DB_CONFIG_FILEPATH,
)
@click.option(
    "--overwrite", "-o",
    help="Flag that will allow overwriting an existing configuration file",
    is_flag=True,
)
def init(filepath, overwrite):
    """ Create a new config file to define database connection parameters. """

    make_config_file(filepath=filepath, overwrite=overwrite)
    _console.print(f":bottle_with_popping_cork: You can now configure your PgSQL connections at {filepath}")


# BACK UP ALL DATABASES ON A GIVEN HOST
# -------------------------------------


@main.command()
@click.argument("host", default="localhost")
@click.option(
    "--folder", "-f",
    help="Folder where the output SQL files will be stored."
)
def db_backup_all(host, folder):
    """Back all databases up on a given HOST
    using PostgreSQL().db_export_pgdump_file()

    HOST can be any named profile in the configuration
    file.

    HOST defaults to localhost
    """

    _console.print(f":direct_hit: Backing up databases on: {host}")

    # Connect to the cluster's master database
    this_cluster = configurations()[host]
    super_db_name = this_cluster['super_db']
    super_db = PostgreSQL(super_db_name, **this_cluster)

    # Make a folder in the outbox with this host's name
    if folder:
        output_folder = Path(folder)
    else:
        output_folder = super_db.DATA_OUTBOX / host
    if not output_folder.exists():
        output_folder.mkdir(parents=True)

    # Loop through all dbs on cluster, exporting all except the super db
    all_dbs = super_db.all_databases_on_cluster_as_list()

    with RichProgress(console=_console) as progress:
        task = progress.add_task(
                total=len(all_dbs),
                description=f'Exporting {len(all_dbs)} dbs'
        )
        for dbname in all_dbs:
            if dbname != super_db_name:
                db = PostgreSQL(dbname, **this_cluster)
                db.db_export_pgdump_file(output_folder)

                progress.advance(task)


# BACK UP A SINGLE DATABASE
# -------------------------


@main.command()
@click.argument("database_name")
@click.argument("host", default="localhost")
@click.option(
    "--folder", "-f",
    help="Folder where the output SQL file will be stored."
)
def db_backup_single(database_name, host, folder):
    """Back up DATABASE_NAME from HOST (to an optional FOLDER)

    HOST can be any named profile in the configuration
    file and defaults to localhost.

    FOLDER will be the default data outbox unless specified
    """

    _console.print(f":direct_hit: Backing up {database_name} from {host}")

    # Connect to the cluster's master database
    this_cluster = configurations()[host]
    super_db_name = this_cluster['super_db']
    super_db = PostgreSQL(super_db_name, **this_cluster)

    # Make a folder in the outbox with this host's name
    if folder:
        output_folder = Path(folder)
    else:
        output_folder = super_db.DATA_OUTBOX / host
    if not output_folder.exists():
        output_folder.mkdir(parents=True)

    # Loop through all dbs on cluster, exporting all except the super db
    for dbname in super_db.all_databases_on_cluster_as_list():
        if dbname != super_db_name:
            db = PostgreSQL(dbname, **this_cluster)
            db.db_export_pgdump_file(output_folder)


if __name__ == "__main__":
    main()
