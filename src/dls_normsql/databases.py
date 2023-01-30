# Use standard logging in this module.
import logging

# Utilities.
from dls_utilpack.require import require

# Types.
from dls_normsql.constants import ClassTypes

# Exceptions.
from dls_normsql.exceptions import NotFound

logger = logging.getLogger(__name__)


class Databases:
    """
    List of available databases.
    """

    # ----------------------------------------------------------------------------------------
    def build_object(self, specification):
        """"""

        class_type = require("database specification", specification, "type")
        database_class = self.lookup_class(class_type)

        try:
            database_object = database_class(specification)
        except Exception as exception:
            raise RuntimeError(
                "unable to build database object for type %s" % (database_class)
            ) from exception

        return database_object

    # ----------------------------------------------------------------------------------------
    def lookup_class(self, class_type):
        """"""

        if class_type == ClassTypes.AIOSQLITE:
            from dls_normsql.aiosqlite import Aiosqlite

            return Aiosqlite

        raise NotFound("unable to get database class for type %s" % (class_type))
