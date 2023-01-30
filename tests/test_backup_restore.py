import logging

from dls_normsql.constants import ClassTypes, CommonFieldnames
from dls_normsql.databases import Databases
from tests.base_tester import BaseTester
from tests.my_table_definition import MyTableDefinition

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------------------------
class TestBackupRestore:
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

        all_sql = "SELECT * FROM my_table"

        try:
            # Connect to database.
            await database.connect()

            # Write one record.
            await database.insert(
                "my_table",
                [{CommonFieldnames.UUID: "a0", "my_field": "{'a': 'f000'}"}],
            )

            # Backup.
            await database.backup()

            # Bulk insert more records.
            insertable_records = []
            bulk_count = 10
            for i in range(bulk_count):
                insertable_records.append(
                    {"uuid": "b%03d" % (i), "my_field": "{'b': '%03d'}" % (i)}
                )
            await database.insert("my_table", insertable_records)

            # Backup again (with bulk records)
            await database.backup()

            # Restore one in the past (when it had a single record).
            await database.restore(1)
            records = await database.query(all_sql)
            assert len(records) == 1

            # Restore most recent (bulk+1 records).
            await database.restore(0)
            records = await database.query(all_sql)
            assert len(records) == bulk_count + 1

            # Bulk insert more records.
            insertable_records = []
            for i in range(bulk_count):
                insertable_records.append(
                    {"uuid": "c%03d" % (i), "my_field": "{'c': '%03d'}" % (i)}
                )
            await database.insert("my_table", insertable_records)
            records = await database.query(all_sql)
            n = len(records)
            assert n == 2 * bulk_count + 1, "2 * bulk + 1"

            # Backup again (with 2*bulk+1 records)
            await database.backup()

            # Restore two in the past (when it had 1 record).
            await database.restore(2)
            records = await database.query(all_sql)
            n = len(records)
            assert n == 1, "restored two in the past"

            # Write one record.
            await database.insert(
                "my_table",
                [{CommonFieldnames.UUID: "d0", "my_field": "{'a': 'd000'}"}],
            )

            # Backup again (with 2 records, pruning the bulk ones).
            await database.backup()

            # Restore top non-pruned backup (when it had 2 records).
            await database.restore(0)
            records = await database.query(all_sql)
            n = len(records)
            assert n == 2, "restored zero in the past after pruning"

            # Restore last non-pruned backup (when it had 1 record).
            await database.restore(1)
            records = await database.query(all_sql)
            n = len(records)
            assert n == 1, "restored one in the past after pruning"

        finally:
            # Connect from the database... necessary to allow asyncio loop to exit.
            await database.disconnect()
