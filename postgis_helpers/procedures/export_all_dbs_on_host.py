"""
Summary of ``export_all_dbs_on_host.py``
----------------------------------------

This script provides a command-line utility that will
create a backup of every database on a SQL cluster,
except for the super database.

Usage
-----

    $ export_all_dbs_on_host HOSTNAME

Note: This requires you to use the ``~/.postgis_helpers``
configuration file. ``HOSTNAME`` is any profile defined
in this file within square brackets. Default options
include ``localhost`` and ``digitalocean``.

"""
import click
from postgis_helpers import PostgreSQL, configurations


@click.command()
@click.argument("host")
def back_up_entire_cluster(host):

    click.echo(f"BACKING UP ALL DATABASES ON {host}")

    # Connect to the cluster's master database
    this_cluster = configurations()[host]
    super_db_name = this_cluster['super_db']
    super_db = PostgreSQL(super_db_name, **this_cluster)

    # Make a folder in the outbox with this host's name
    output_folder = super_db.DATA_OUTBOX / host
    if not output_folder.exists():
        output_folder.mkdir(parents=True)

    # Loop through all dbs on cluster, exporting all except the super db
    for dbname in super_db.all_databases_on_cluster_as_list():
        if dbname != super_db_name:
            db = PostgreSQL(dbname, **this_cluster)
            db.db_export_pgdump_file(output_folder)
