import logging
from collections import OrderedDict

logger = logging.getLogger(__name__)


class TableDefinition:
    """
    Base class for table definitions.
    This doesn't do much right now, but is here to support future capabilities.
    """

    def __init__(self, name):
        self.name = name
        self.fields = OrderedDict()
