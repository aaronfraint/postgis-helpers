from ward import fixture

from postgis_helpers import PostgreSQL, configurations


@fixture(scope="global")
def database_local():

    # Set up the database
    db = PostgreSQL("test_from_ward",
                    verbosity="minimal",
                    **configurations()["localhost"])
    db.create()

    # Yield to the test assertion
    yield db

    # Tear down the database
    db.delete()


@fixture(scope="global")
def database_remote():

    # Set up the database
    db = PostgreSQL("test_from_ward",
                    verbosity="minimal",
                    **configurations()["digitalocean"])
    db.create()

    # Yield to the test assertion
    yield db

    # Tear down the database
    db.delete()
