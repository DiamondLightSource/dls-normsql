import logging

import pytest
from dls_utilpack.envvar import Envvar

from dls_normsql.constants import ClassTypes, RevisionFieldnames, Tablenames
from dls_normsql.databases import Databases
from tests.base_tester import BaseTester
from tests.my_database_definition import MyDatabaseDefinition

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------------------------
class TestRevisionAiosqlite:
    def test(self, logging_setup, output_directory):
        """
        Tests the sqlite implementation of Database.
        """

        # Database specification.
        database_specification = {
            "type": ClassTypes.AIOSQLITE,
            "filename": f"{output_directory}/database.sqlite",
        }

        # Test direct SQL access to the database.
        RevisionTester().main(
            database_specification,
            output_directory,
        )


# ----------------------------------------------------------------------------------------
class TestRevisionAiomysql:
    def test(self, logging_setup, output_directory):
        """
        Tests the mysql implementation of Database.
        """

        host = Envvar("MYSQL_HOST", default="127.0.0.1")
        assert host.is_set
        port = Envvar("MYSQL_PORT", default=3306)
        assert port.is_set

        # Database specification.
        database_specification = {
            "type": ClassTypes.AIOMYSQL,
            "type_specific_tbd": {
                "database_name": "dls_normsql_pytest",
                "host": "$MYSQL_HOST",
                "port": "$MYSQL_PORT",
                "username": "root",
                "password": "root",
            },
        }

        # Test direct SQL access to the database.
        RevisionTester().main(
            database_specification,
            output_directory,
        )


# ----------------------------------------------------------------------------------------
class RevisionTester(BaseTester):
    """
    Test direct SQL access to the database.
    """

    async def _main_coroutine(self, database_specification, output_directory):
        """ """

        database_definition_object = MyDatabaseDefinition()
        databases = Databases()
        database1 = databases.build_object(
            database_specification, database_definition_object
        )

        try:
            # Connect to database.
            await database1.connect(should_drop_database=True)

            with pytest.raises(Exception):
                # We should not have the mytable2.
                records = await database1.query("SELECT * FROM my_table2")

            # Back level the database's revision.
            await database1.update(
                Tablenames.REVISION, {RevisionFieldnames.NUMBER: "3"}, "1=1"
            )

            # Apply the revisions up to the definition revision which is 4.
            await database1.apply_revisions()

            # Now we should have the mytable2.
            records = await database1.query("SELECT * FROM my_table2")
            assert len(records) == 0

        finally:
            # Disonnect from the databases... necessary to allow asyncio loop to exit.
            await database1.disconnect()
