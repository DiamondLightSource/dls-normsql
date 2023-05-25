import logging

import aiomysql

from tests.base_tester import BaseTester

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------------------------
class TestAiomysql:
    def test(self, logging_setup, output_directory):
        """
        Tests the sqlite implementation of Database.
        """

        # Database specification.
        database_specification = {}

        # Test direct SQL access to the database.
        AiomysqlTester().main(
            database_specification,
            output_directory,
        )


# ----------------------------------------------------------------------------------------
class AiomysqlTester(BaseTester):
    """
    Test direct SQL access to the database.
    """

    async def _main_coroutine(self, database_specification, output_directory):
        """ """
        pool = await aiomysql.create_pool(
            host="docker-mysql",
            port=3306,
            user="root",
            password="root",
            db="mysql",
        )

        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT 42;")
                logger.debug(cursor.description)
                (r,) = await cursor.fetchone()
                assert r == 42

        pool.close()
        await pool.wait_closed()
