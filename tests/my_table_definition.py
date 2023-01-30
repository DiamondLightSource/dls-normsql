from dls_normsql.constants import CommonFieldnames
from dls_normsql.table_definition import TableDefinition


# ----------------------------------------------------------------------------------------
class MyTableDefinition(TableDefinition):
    """
    A database table definition for testing.
    """

    def __init__(self):
        TableDefinition.__init__(self, "my_table")

        # All cookies have a unique uuid field.
        self.fields[CommonFieldnames.UUID] = {
            "type": "TEXT PRIMARY KEY",
            "index": True,
        }

        self.fields[CommonFieldnames.CREATED_ON] = {"type": "TEXT", "index": True}
        self.fields["my_field"] = {"type": "TEXT", "index": True}

        for i in range(200):
            self.fields["my_other_field_%03d" % (i)] = {"type": "TEXT", "index": True}
