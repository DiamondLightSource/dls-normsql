import logging

from dls_normsql.constants import ClassTypes, CommonFieldnames
from dls_normsql.databases import Databases
from tests.base_tester import BaseTester
from tests.my_table_definition import MyTableDefinition

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------------------------
class TestDatabase:
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
class DatabaseTester(BaseTester):
    """
    Test direct SQL access to the database.
    """

    async def _main_coroutine(self, database_specification, output_directory):
        """ """

        databases = Databases()
        database = databases.build_object(database_specification)
        database.add_table_definition(MyTableDefinition())

        try:
            # Connect to database.
            await database.connect()

            # Write one record.
            await database.insert(
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
            records = await database.query(all_sql)
            assert len(records) == 2

            # Bulk insert more records.
            insertable_records = [
                ["f1", "{'a': 'f111'}"],
                ["f2", "{'a': 'f112'}"],
                ["f3", "{'a': 'f113'}"],
                ["f4", "{'a': 'f114'}"],
            ]
            await database.execute(
                f"INSERT INTO my_table"
                f" ({CommonFieldnames.UUID}, my_field)"
                " VALUES (?, ?)",
                insertable_records,
            )

            # ------------------------------------------------------------
            # Single insert another record.
            insertable_record = ["z1", "{'z': 'z111'}"]

            await database.execute(
                f"INSERT INTO my_table"
                f" ({CommonFieldnames.UUID}, my_field)"
                " VALUES (?, ?)",
                insertable_record,
                should_commit=False,
            )

            # Roll it back.
            await database.rollback()

            # Query all the records.
            all_sql = "SELECT * FROM my_table"
            records = await database.query(all_sql)
            assert len(records) == 6

            # ------------------------------------------------------------
            # Disonnect from the database.
            await database.disconnect()

            # Reconnect to database.
            await database.connect()

            # Query all the records.
            all_sql = "SELECT * FROM my_table"
            records = await database.query(all_sql)
            assert len(records) == 6

        finally:
            # Connect from the database... necessary to allow asyncio loop to exit.
            await database.disconnect()
