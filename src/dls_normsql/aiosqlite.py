import asyncio
import glob

# This class produces log entries.
import logging
import os
import re
import shutil
from collections import OrderedDict
from datetime import datetime
from typing import Optional

import aiosqlite

# Utilities.
from dls_utilpack.callsign import callsign
from dls_utilpack.explain import explain
from dls_utilpack.isodatetime import isodatetime_filename
from dls_utilpack.require import require

from dls_normsql.constants import CommonFieldnames, RevisionFieldnames, Tablenames
from dls_normsql.table_definition import TableDefinition

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------------------------
def sqlite_regexp_callback(pattern, input):
    reg = re.compile(pattern)
    return reg.search(input) is not None


# ----------------------------------------------------------------------------------------
class Aiosqlite:
    """
    Class with coroutines for creating and querying a sqlite database.
    """

    # ----------------------------------------------------------------------------------------
    def __init__(self, specification):
        """
        Construct object.  Do not connect to database.
        """
        self.__filename = require(
            f"{callsign(self)} specification", specification, "filename"
        )

        # Backup directory default is the path where the filename is.
        self.__backup_directory = specification.get(
            "backup_directory", os.path.dirname(self.__filename)
        )

        # Don't normally want to see all the debug for aiosqlite internals.
        level = specification.get("log_level", "INFO")
        logging.getLogger("aiosqlite").setLevel(level)

        self.__connection = None

        self.__tables = {}

        self.LATEST_REVISION = 1

        self.__backup_restore_lock = asyncio.Lock()

        # Last undo position.
        self.__last_restore = 0

    # ----------------------------------------------------------------------------------------
    async def connect(self):
        """
        Connect to database at filename given in constructor.
        """

        should_create_schemas = False

        # File doesn't exist yet?
        if not os.path.isfile(self.__filename):
            # Create directory for the file.
            await self.__create_directory(self.__filename)
            # After connection, we must create the schemas.
            should_create_schemas = True

        logger.debug(f"connecting to {self.__filename}")

        self.__connection = await aiosqlite.connect(self.__filename)
        self.__connection.row_factory = aiosqlite.Row

        # rows = await self.query("PRAGMA journal_mode", why="query journal mode")
        # logger.debug(f"journal mode rows {json.dumps(rows)}")

        # rows = await self.query("PRAGMA journal_mode=OFF", why="turn OFF journal mode")
        # logger.debug(f"journal mode OFF rows {json.dumps(rows)}")

        # rows = await self.query("PRAGMA journal_mode", why="query journal mode")
        # logger.debug(f"journal mode rows {json.dumps(rows)}")

        # rows = await self.query("SELECT * from mainTable", why="main table check")

        await self.__connection.create_function("regexp", 2, sqlite_regexp_callback)
        logger.debug("created regexp function")

        await self.add_table_definitions()

        if should_create_schemas:
            await self.create_schemas()
            await self.insert(Tablenames.REVISION, [{"number": self.LATEST_REVISION}])
            # TODO: Set permission on sqlite file from configuration.
            os.chmod(self.__filename, 0o666)
        else:
            try:
                records = await self.query(
                    f"SELECT number FROM {Tablenames.REVISION}",
                    why="get database revision",
                )
                if len(records) == 0:
                    old_revision = 0
                else:
                    old_revision = records[0]["number"]
            except Exception as exception:
                logger.warning(
                    f"could not get revision, presuming legacy database with no table: {exception}"
                )
                old_revision = 0

            if old_revision < self.LATEST_REVISION:
                logger.debug(
                    f"need to update old revision {old_revision}"
                    f" to latest revision {self.LATEST_REVISION}"
                )
                for revision in range(old_revision, self.LATEST_REVISION):
                    logger.debug(f"updating to revision {revision+1}")
                    await self.apply_revision(revision + 1)
                await self.update(
                    Tablenames.REVISION,
                    {"number": self.LATEST_REVISION},
                    "1 = 1",
                    why="update database revision",
                )

        # Emit the name of the database file for positive confirmation on console.
        logger.info(
            f"{callsign(self)} database file is {self.__filename} revision {self.LATEST_REVISION}"
        )

    # ----------------------------------------------------------------------------------------
    async def apply_revision(self, revision):
        logger.debug(f"updating to revision {revision}")
        # Updating to revision 1 presumably means
        # this is a legacy database with no revision table in it.
        if revision == 1:
            logger.info(f"creating {Tablenames.REVISION} table")
            await self.create_table(Tablenames.REVISION)
            await self.insert(Tablenames.REVISION, [{"revision": revision}])

    # ----------------------------------------------------------------------------------------
    async def disconnect(self):

        if self.__connection is not None:
            logger.debug(f"{callsign(self)} disconnecting")
            await self.__connection.close()
            self.__connection = None

    # ----------------------------------------------------------------------------------------
    async def __create_directory(self, filename):

        directory, filename = os.path.split(filename)

        if not os.path.exists(directory):
            # Make sure that parent directories which get created will have public permission.
            umask = os.umask(0)
            os.umask(umask & ~0o0777)
            os.makedirs(directory)
            os.umask(umask)

    # ----------------------------------------------------------------------------------------
    def add_table_definition(self, table_definition):

        self.__tables[table_definition.name] = table_definition

    # ----------------------------------------------------------------------------------------
    async def add_table_definitions(self):

        self.add_table_definition(RevisionTableDefinition(self))

    # ----------------------------------------------------------------------------------------
    async def commit(self):
        """
        Commit transaction, if any outstanding.
        """

        if self.__connection.in_transaction:
            await self.__connection.commit()

    # ----------------------------------------------------------------------------------------
    async def rollback(self):
        """
        Roll back transaction, if any outstanding.
        """

        if self.__connection.in_transaction:
            await self.__connection.rollback()

    # ----------------------------------------------------------------------------------------
    async def create_schemas(self, should_commit: Optional[bool] = True):

        for table in self.__tables.values():
            await self.create_table(table, should_commit=False)

        if should_commit:
            await self.__connection.commit()

    # ----------------------------------------------------------------------------------------
    async def create_table(self, table, should_commit: Optional[bool] = True):
        """
        Wipe and re-create the table in the database.
        """

        # If table is a string, presume it's a table name.
        if isinstance(table, str):
            table = require("table definitions", self.__tables, table)

        await self.__connection.execute("DROP TABLE IF EXISTS %s" % (table.name))

        fields_sql = []
        indices_sql = []

        for field_name in table.fields:
            field = table.fields[field_name]
            fields_sql.append("%s %s" % (field_name, field["type"]))
            if field.get("index"):
                indices_sql.append(
                    "CREATE INDEX %s_%s ON %s(%s)"
                    % (table.name, field_name, table.name, field_name)
                )

        await self.__connection.execute(
            "CREATE TABLE %s(%s)" % (table.name, ", ".join(fields_sql))
        )

        for sql in indices_sql:
            await self.__connection.execute(sql)

        if should_commit:
            await self.__connection.commit()

    # ----------------------------------------------------------------------------------------
    async def insert(
        self,
        table,
        rows,
        why=None,
        should_commit: Optional[bool] = True,
    ):
        """
        Insert one or more rows.
        Each row is a dictionary.
        The first row is expected to define the keys for all rows inserted.
        Keys in the rows are ignored if not defined in the table schema.
        Table schema columns not specified in the first row's keys will get their sql-defined default values.
        """

        if len(rows) == 0:
            return

        # If table is a string, presume it's a table name.
        if isinstance(table, str):
            table = require("table definitions", self.__tables, table)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

        values_rows = []

        insertable_fields = []
        for field in table.fields:
            # The first row is expected to define the keys for all rows inserted.
            if field in rows[0]:
                insertable_fields.append(field)
            elif field == CommonFieldnames.CREATED_ON:
                insertable_fields.append(field)

        qmarks = ["?"] * len(insertable_fields)

        for row in rows:
            values_row = []
            for field in table.fields:
                if field in row:
                    values_row.append(row[field])
                elif field == CommonFieldnames.CREATED_ON:
                    values_row.append(row.get(field, now))
            values_rows.append(values_row)

        sql = "INSERT INTO %s (%s) VALUES (%s)" % (
            table.name,
            ", ".join(insertable_fields),
            ", ".join(qmarks),
        )

        try:
            await self.__connection.executemany(sql, values_rows)

            if why is None:
                logger.debug("%s %s" % (sql, values_rows))
            else:
                logger.debug("%s: %s %s" % (why, sql, values_rows))

            if should_commit:
                await self.__connection.commit()

        except aiosqlite.OperationalError:
            if why is None:
                raise RuntimeError(f"failed to execute {sql}")
            else:
                raise RuntimeError(f"failed to execute {why}: {sql}")

    # ----------------------------------------------------------------------------------------
    async def update(
        self,
        table,
        row,
        where,
        subs=None,
        why=None,
        should_commit: Optional[bool] = True,
    ):
        """
        Update specified fields to all rows matching selection.
        """

        # If table is a string, presume it's a table name.
        if isinstance(table, str):
            table = require("table definitions", self.__tables, table)

        values_row = []
        qmarks = []

        for field in table.fields:
            if field == CommonFieldnames.UUID or field == CommonFieldnames.AUTOID:
                continue
            if field not in row:
                continue
            qmarks.append("%s = ?" % (field))
            values_row.append(row[field])

        if len(values_row) == 0:
            raise RuntimeError("no fields in record match database table")

        sql = "UPDATE %s SET %s WHERE %s" % (
            table.name,
            ", ".join(qmarks),
            where,
        )

        if subs is not None:
            values_row.extend(subs)

        try:
            cursor = await self.__connection.execute(sql, values_row)
            rowcount = cursor.rowcount

            if should_commit:
                await self.__connection.commit()

            if why is None:
                logger.debug("%d rows from %s\nvalues %s" % (rowcount, sql, values_row))
            else:
                logger.debug(
                    "%d rows from %s: %s\nvalues %s" % (rowcount, why, sql, values_row)
                )

        except aiosqlite.OperationalError:
            if why is None:
                raise RuntimeError(f"failed to execute {sql}")
            else:
                raise RuntimeError(f"failed to execute {why}: {sql}")

        return rowcount

    # ----------------------------------------------------------------------------------------
    async def execute(
        self, sql, subs=None, why=None, should_commit: Optional[bool] = True
    ):
        """
        Execute a sql statement.
        If subs is a list of lists, then these are presumed the values for executemany.
        """

        cursor = None
        try:
            # Subs is a list of lists?
            if isinstance(subs, list) and len(subs) > 0 and isinstance(subs[0], list):
                logger.debug(f"inserting {len(subs)} of {len(subs[0])}")
                cursor = await self.__connection.executemany(sql, subs)
            else:
                cursor = await self.__connection.execute(sql, subs)

            if should_commit:
                await self.__connection.commit()

            if why is None:
                if cursor.rowcount > 0:
                    logger.debug(
                        f"{cursor.rowcount} records affected by\n{sql} values {subs}"
                    )
                else:
                    logger.debug(f"{sql} values {subs}")
            else:
                if cursor.rowcount > 0:
                    logger.debug(
                        f"{cursor.rowcount} records affected by {why}:\n{sql} values {subs}"
                    )
                else:
                    logger.debug(f"{why}: {sql} values {subs}")
        except aiosqlite.OperationalError:
            if why is None:
                raise RuntimeError(f"failed to execute {sql}")
            else:
                raise RuntimeError(f"failed to execute {why}: {sql}")

    # ----------------------------------------------------------------------------------------
    async def query(self, sql, subs=None, why=None):

        if subs is None:
            subs = {}

        cursor = None
        try:
            cursor = await self.__connection.cursor()
            await cursor.execute(sql, subs)
            rows = await cursor.fetchall()
            cols = []
            for col in cursor.description:
                cols.append(col[0])

            if why is None:
                logger.debug("%d records from: %s" % (len(rows), sql))
            else:
                logger.debug("%d records from %s: %s" % (len(rows), why, sql))
            records = []
            for row in rows:
                record = OrderedDict()
                for index, col in enumerate(cols):
                    record[col] = row[index]
                records.append(record)
            return records
        except aiosqlite.OperationalError as exception:
            if why is None:
                raise RuntimeError(explain(exception, f"executing {sql}"))
            else:
                raise RuntimeError(explain(exception, f"executing {why}: {sql}"))
        finally:
            if cursor is not None:
                await cursor.close()

    # ----------------------------------------------------------------------------------------
    async def backup(self):
        """
        Back up database to timestamped location.
        """

        # Prune all the restores which were orphaned.
        directory = self.__backup_directory

        basename, suffix = os.path.splitext(os.path.basename(self.__filename))

        filenames = glob.glob(f"{directory}/{basename}.*{suffix}")

        filenames.sort(reverse=True)

        logger.debug(f"[BACKPRU] {self.__last_restore} is last restore")
        for restore in range(self.__last_restore):
            logger.debug(
                f"[BACKPRU] removing {restore}-th restore {filenames[restore]}"
            )
            os.remove(filenames[restore])

        self.__last_restore = 0

        async with self.__backup_restore_lock:
            timestamp = isodatetime_filename()
            to_filename = f"{directory}/{basename}.{timestamp}{suffix}"

            await self.disconnect()
            try:
                await self.__create_directory(to_filename)
                shutil.copy2(self.__filename, to_filename)
                logger.debug(f"backed up to {to_filename}")
            except Exception:
                raise RuntimeError(f"copy {self.__filename} to {to_filename} failed")
            finally:
                await self.connect()

    # ----------------------------------------------------------------------------------------
    async def restore(self, nth):
        """
        Restore database from timestamped location.
        """

        async with self.__backup_restore_lock:
            directory = self.__backup_directory

            basename, suffix = os.path.splitext(os.path.basename(self.__filename))

            filenames = glob.glob(f"{directory}/{basename}.*{suffix}")

            filenames.sort(reverse=True)

            if nth >= len(filenames):
                raise RuntimeError(
                    f"restoration index {nth} is more than available {len(filenames)}"
                )

            from_filename = filenames[nth]

            await self.disconnect()
            try:
                shutil.copy2(from_filename, self.__filename)
                logger.debug(
                    f"restored nth {nth} out of {len(filenames)} from {from_filename}"
                )
            except Exception:
                raise RuntimeError(f"copy {from_filename} to {self.__filename} failed")
            finally:
                await self.connect()

            self.__last_restore = nth


# ----------------------------------------------------------------------------------------
class RevisionTableDefinition(TableDefinition):
    # ----------------------------------------------------------------------------------------
    def __init__(self, database):
        TableDefinition.__init__(self, "revision")

        self.fields[RevisionFieldnames.CREATED_ON] = {"type": "TEXT", "index": True}
        self.fields[RevisionFieldnames.NUMBER] = {"type": "INTEGER", "index": False}
