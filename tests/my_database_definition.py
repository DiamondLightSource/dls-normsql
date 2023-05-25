import logging

from tests.my_table_definition import MyTableDefinition

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------------------------
class MyDatabaseDefinition:
    """
    Class which defines the database tables and revision migration path.
    Used in concert with the normsql class.
    """

    # ----------------------------------------------------------------------------------------
    def __init__(self):
        """
        Construct object.  Do not connect to database.
        """

        self.LATEST_REVISION = 4

    # ----------------------------------------------------------------------------------------
    async def apply_revision(self, database, revision):

        logger.debug(f"applying revision {revision}")

    # ----------------------------------------------------------------------------------------
    async def add_table_definitions(self, database):
        """
        Make all the table definitions.
        """

        # Table schemas in our database.
        database.add_table_definition(MyTableDefinition())
