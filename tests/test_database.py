import logging

from dls_utilpack.envvar import Envvar

from dls_normsql.constants import ClassTypes, CommonFieldnames
from dls_normsql.databases import Databases
from tests.base_tester import BaseTester
from tests.my_database_definition import MyDatabaseDefinition

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------------------------
class TestDatabaseAiosqlite:
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
        DatabaseTester().main(
            database_specification,
            output_directory,
        )


# ----------------------------------------------------------------------------------------
class TestDatabaseAiomysql:
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
        DatabaseTester().main(
            database_specification,
            output_directory,
        )


# ----------------------------------------------------------------------------------------
class DatabaseTester(BaseTester):
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
        database2 = databases.build_object(
            database_specification, database_definition_object
        )

        try:
            # Connect to database.
            await database1.connect(should_drop_database=True)

            # Connect to second database.
            await database2.connect()

            # Write one record.
            await database1.insert(
                "my_table",
                [
                    {
                        CommonFieldnames.UUID: "x0",
                        "my_field": "{'a': 'x000'}",
                    },
                    {
                        CommonFieldnames.UUID: "x1",
                        "my_field": "{'a': 'x001'}",
                    },
                ],
            )

            # Query all the records.
            all_sql = "SELECT * FROM my_table"
            records = await database1.query(all_sql)
            assert len(records) == 2

            # Bulk insert more records to test multiple substitutions.
            insertable_records = [
                ["f1", "{'a': 'f111'}"],
                ["f2", "{'a': 'f112'}"],
                ["f3", "{'a': 'f113'}"],
                ["f4", "{'a': 'f114'}"],
            ]
            await database1.execute(
                f"INSERT INTO my_table"
                f" ({CommonFieldnames.UUID}, my_field)"
                " VALUES (?, ?)",
                insertable_records,
            )

            # ------------------------------------------------------------
            await database1.begin()

            # Single insert another record.
            insertable_record = ["y1", "{'y': 'y111'}"]

            await database1.execute(
                f"INSERT INTO my_table"
                f" ({CommonFieldnames.UUID}, my_field)"
                " VALUES (?, ?)",
                insertable_record,
            )

            # Roll it back.
            await database1.rollback()

            # Query all the records.
            all_sql = "SELECT * FROM my_table"
            records = await database1.query(all_sql)
            assert len(records) == 6

            # ------------------------------------------------------------
            await database1.begin()

            # Single insert another record.
            insertable_record = ["z1", "{'z': 'z111'}"]

            await database1.execute(
                f"INSERT INTO my_table"
                f" ({CommonFieldnames.UUID}, my_field)"
                " VALUES (?, ?)",
                insertable_record,
            )

            # Query all the records from second database, should not see the uncommitted insert.
            records = await database2.query(all_sql)
            assert len(records) == 6

            # Commit it.
            await database1.commit()

            # Query all the records.
            records = await database1.query(all_sql)
            assert len(records) == 7

            # Query all the records from second database, should now see the committed insert.
            records = await database2.query(all_sql)
            assert len(records) == 7

            # ------------------------------------------------------------
            # Disconnect from the databases.
            await database1.disconnect()

            logger.debug("------------------------------------------")

            # Reconnect on the same database object.
            await database1.connect()

            # Query all the records.
            records = await database1.query(all_sql)
            assert len(records) == 7

        finally:
            # Disonnect from the databases... necessary to allow asyncio loop to exit.
            await database2.disconnect()
            await database1.disconnect()
