import asyncio
import glob

# This class produces log entries.
import logging
import os
import shutil
import warnings
from collections import OrderedDict
from datetime import datetime
from typing import Any, List

import aiomysql

# Utilities.
from dls_utilpack.callsign import callsign
from dls_utilpack.envvar import Envvar
from dls_utilpack.explain import explain
from dls_utilpack.isodatetime import isodatetime_filename
from dls_utilpack.require import require

from dls_normsql.constants import CommonFieldnames, RevisionFieldnames, Tablenames
from dls_normsql.table_definition import TableDefinition

logger = logging.getLogger(__name__)

connect_lock = asyncio.Lock()
apply_revisions_lock = asyncio.Lock()


# ----------------------------------------------------------------------------------------
class Aiomysql:
    """
    Class with coroutines for creating and querying a sqlite database.
    """

    # ----------------------------------------------------------------------------------------
    def __init__(self, specification, database_definition_object):
        """
        Construct object.  Do not connect to database.
        """
        self.__database_definition_object = database_definition_object

        s = f"{callsign(self)} specification"

        t = require(s, specification, "type_specific_tbd")

        # We might use values in type_specific_tbd during the connection method.
        self.__type_specific_tbd = t

        # We will do environment variable substitution for host and port if they start with $.
        self.__host = require(s, t, "host")
        if self.__host.startswith("$"):
            envvar = Envvar(self.__host[1:], default="127.0.0.1")
            if not envvar.is_set:
                raise RuntimeError(
                    f"configuration error: environment variable {self.__host[1:]} is not set"
                )
            self.__host = envvar.value

        self.__port = require(s, t, "port")
        if self.__port.startswith("$"):
            envvar = Envvar(self.__port[1:], default=3306)
            if not envvar.is_set:
                raise RuntimeError(
                    f"configuration error: environment variable {self.__port[1:]} is not set"
                )
            self.__port = int(envvar.value)

        self.__username = require(s, t, "username")

        self.__password = require(s, t, "password")

        self.__database_name = require(s, t, "database_name")

        self.__connection = None

        self.__tables = {}

        # Deriving class has not established its latest revision?
        if not hasattr(self, "LATEST_REVISION") or self.LATEST_REVISION is None:
            # Presume it is 1.
            self.LATEST_REVISION = 1

        self.__backup_restore_lock = asyncio.Lock()

        # Last undo position.
        self.__last_restore = 0

    # ----------------------------------------------------------------------------------------
    def __parameterize(self, sql: str, subs: List[Any]):
        """
        Connect to database at filename given in constructor.
        """

        sql = sql.replace("?", "%s")

        return sql

    # ----------------------------------------------------------------------------------------
    async def connect(self, should_drop_database=False):
        """
        Connect to database at filename given in constructor.
        """

        async with connect_lock:
            should_create_schemas = False

            logger.debug(f"connecting to mysql {self.__host}:{self.__port}")

            self.__connection = await aiomysql.connect(
                host=self.__host,
                port=self.__port,
                user=self.__username,
                password=self.__password,
            )

            # Allow the possibility to set the transaction isolation level so that commits on one connection are available to other connections quickly.
            # The default was REPEATABLE READ, which is stricter, but I found I could not get committed data immediately at a second connection.
            # TODO: Consider implications of setting aiomysql to the more relaxed TRANSACTION ISOLATION LEVEL READ COMMITTED.
            isolation_level = self.__type_specific_tbd.get(
                "isolation_level", "READ COMMITTED"
            )
            await self.execute(
                f"SET GLOBAL TRANSACTION ISOLATION LEVEL {isolation_level}"
            )

            logger.debug(f"autocommit is {self.__connection.get_autocommit()}")
            try:
                await self.execute(f"USE {self.__database_name}")
                database_exists = True
            except RuntimeError:
                database_exists = False

            if database_exists and should_drop_database:
                await self.execute(f"DROP DATABASE {self.__database_name}")
                database_exists = False

            if not database_exists:
                await self.execute(f"CREATE DATABASE {self.__database_name}")
                await self.execute(f"USE {self.__database_name}")
                should_create_schemas = True

            # Let the base class contribute its table definitions to the in-memory list.
            await self.add_table_definitions()

            if should_create_schemas:
                await self.create_schemas()
                await self.insert(
                    Tablenames.REVISION, [{"number": self.LATEST_REVISION}]
                )

            # Emit the name of the database file for positive confirmation on console.
            logger.info(
                f"{callsign(self)} database name is {self.__database_name} code revision {self.LATEST_REVISION}"
            )

    # ----------------------------------------------------------------------------------------
    async def apply_revisions(self):
        """
        Apply revision updates to databse if needed.
        """

        # TODO: Consider how to lock database while running applying_revisions.
        # TODO: Establish transaction arouund apply_revisions with rollback if error.
        async with apply_revisions_lock:
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
                # Backup before applying revisions.
                logger.debug(
                    f"[BKREVL] backing up before updating from revision {old_revision} to revision {self.LATEST_REVISION}"
                )

                await self.backup()

                for revision in range(old_revision, self.LATEST_REVISION):
                    logger.debug(f"updating to revision {revision+1}")
                    await self.apply_revision(revision + 1)
                await self.update(
                    Tablenames.REVISION,
                    {"number": self.LATEST_REVISION},
                    "1 = 1",
                    why="update database revision",
                )
            else:
                logger.debug(
                    f"[BKREVL] no need to update persistent revision {old_revision}"
                    f" which matches code revision {self.LATEST_REVISION}"
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
            # Commit final transaction if not currently autocommitting.
            if not self.__connection.get_autocommit():
                logger.debug(f"[DISSHU] {callsign(self)} committing final transaction")
                await self.__connection.commit()
            logger.debug(f"[DISSHU] {callsign(self)} closing connection to server")
            self.__connection.close()
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

        # Let the database definition object do its thing.
        await self.__database_definition_object.add_table_definitions(self)

    # ----------------------------------------------------------------------------------------
    async def begin(self):
        """
        Begin transaction.
        """

        logger.debug("beginning transaction")

        await self.__connection.begin()

    # ----------------------------------------------------------------------------------------
    async def commit(self):
        """
        Commit transaction.
        """

        logger.debug("committing transaction")

        await self.__connection.commit()

    # ----------------------------------------------------------------------------------------
    async def rollback(self):
        """
        Roll back transaction.
        """
        logger.debug("rolling back transaction")

        await self.__connection.rollback()

    # ----------------------------------------------------------------------------------------
    async def create_schemas(self):

        for table in self.__tables.values():
            await self.create_table(table)

    # ----------------------------------------------------------------------------------------
    async def create_table(self, table):
        """
        Wipe and re-create the table in the database.
        """

        # If table is a string, presume it's a table name.
        if isinstance(table, str):
            table = require("table definitions", self.__tables, table)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            await self.execute("DROP TABLE IF EXISTS %s" % (table.name))

        async with self.__connection.cursor() as cursor:

            fields_sql = []
            indices_sql = []

            for field_name in table.fields:
                field = table.fields[field_name]
                field_type = field["type"].upper()
                if field_type == "TEXT PRIMARY KEY":
                    field_type = "VARCHAR(64) PRIMARY KEY"
                fields_sql.append("`%s` %s" % (field_name, field_type))
                if field.get("index", False):
                    if field_type == "TEXT":
                        index_length = "(128)"
                    else:
                        index_length = ""
                    indices_sql.append(
                        "CREATE INDEX `%s_%s` ON `%s`(`%s`%s)"
                        % (table.name, field_name, table.name, field_name, index_length)
                    )

            sql = "CREATE TABLE `%s`\n(%s)" % (table.name, ",\n  ".join(fields_sql))

            logger.debug("\n%s\n%s" % (sql, "\n".join(indices_sql)))

            await cursor.execute(sql)

            for sql in indices_sql:
                await cursor.execute(sql)

    # ----------------------------------------------------------------------------------------
    async def insert(
        self,
        table,
        rows,
        why=None,
    ) -> None:
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
                if field == CommonFieldnames.CREATED_ON:
                    created_on = row.get(field)
                    if created_on is None:
                        created_on = now
                    values_row.append(created_on)
                elif field in row:
                    values_row.append(row[field])
            values_rows.append(values_row)

        sql = "INSERT INTO %s\n  (%s)\n  VALUES (%s)" % (
            table.name,
            ", ".join(insertable_fields),
            ", ".join(qmarks),
        )

        sql = self.__parameterize(sql, values_rows)

        if why is None:
            message = "\n%s\n%s" % (sql, values_rows)
        else:
            message = "%s:\n%s\n%s" % (why, sql, values_rows)

        try:
            async with self.__connection.cursor() as cursor:
                await cursor.executemany(sql, values_rows)
                logger.debug(message)

        except (TypeError, aiomysql.OperationalError) as exception:
            raise RuntimeError(f"{exception} doing {message}")

    # ----------------------------------------------------------------------------------------
    async def update(
        self,
        table,
        row,
        where,
        subs=None,
        why=None,
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

        sql = "UPDATE %s SET\n  %s\nWHERE %s" % (
            table.name,
            ",\n  ".join(qmarks),
            where,
        )

        if subs is not None:
            values_row.extend(subs)

        sql = self.__parameterize(sql, subs)

        async with self.__connection.cursor() as cursor:
            try:
                await cursor.execute(sql, values_row)
                rowcount = cursor.rowcount

                if why is None:
                    logger.debug(
                        "%d rows from:\n%s\nvalues %s" % (rowcount, sql, values_row)
                    )
                else:
                    logger.debug(
                        "%d rows from %s:\n%s\nvalues %s"
                        % (rowcount, why, sql, values_row)
                    )

            except (TypeError, aiomysql.OperationalError):
                if why is None:
                    raise RuntimeError(f"failed to execute {sql}")
                else:
                    raise RuntimeError(f"failed to execute {why}: {sql}")

        return rowcount

    # ----------------------------------------------------------------------------------------
    async def execute(
        self,
        sql,
        subs=None,
        why=None,
    ):
        """
        Execute a sql statement.
        If subs is a list of lists, then these are presumed the values for executemany.
        """

        async with self.__connection.cursor() as cursor:
            try:
                sql = self.__parameterize(sql, subs)

                # Subs is a list of lists?
                if (
                    isinstance(subs, list)
                    and len(subs) > 0
                    and isinstance(subs[0], list)
                ):
                    logger.debug(f"inserting {len(subs)} of {len(subs[0])}")
                    await cursor.executemany(sql, subs)
                else:
                    await cursor.execute(sql, subs)

                if why is None:
                    if cursor.rowcount > 0:
                        logger.debug(
                            f"{cursor.rowcount} records affected by\n{sql}\nvalues {subs}"
                        )
                    else:
                        logger.debug(f"{sql}\nvalues {subs}")
                else:
                    if cursor.rowcount > 0:
                        logger.debug(
                            f"{cursor.rowcount} records affected by {why}:\n{sql} values {subs}"
                        )
                    else:
                        logger.debug(f"{why}: {sql}\nvalues {subs}")
            except (TypeError, aiomysql.OperationalError):
                if why is None:
                    raise RuntimeError(f"failed to execute {sql}")
                else:
                    raise RuntimeError(f"failed to execute {why}: {sql}")

    # ----------------------------------------------------------------------------------------
    async def query(self, sql, subs=None, why=None):

        if subs is None:
            subs = {}

        sql = self.__parameterize(sql, subs)

        cursor = None
        async with self.__connection.cursor() as cursor:
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
            except (TypeError, aiomysql.OperationalError) as exception:
                if why is None:
                    raise RuntimeError(explain(exception, f"executing {sql}"))
                else:
                    raise RuntimeError(explain(exception, f"executing {why}: {sql}"))

    # ----------------------------------------------------------------------------------------
    async def backup(self):
        """
        Back up database to timestamped location.
        """

        async with self.__backup_restore_lock:
            # Prune all the restores which were orphaned.
            directory = self.__backup_directory
            if directory is None:
                raise RuntimeError("no backup directory supplied in confirmation")

            basename, suffix = os.path.splitext(os.path.basename(self.__database_name))

            filenames = glob.glob(f"{directory}/{basename}.*{suffix}")

            filenames.sort(reverse=True)

            for restore in range(self.__last_restore):
                logger.debug(
                    f"[BACKPRU] removing {restore}-th restore {filenames[restore]}"
                )
                os.remove(filenames[restore])

            self.__last_restore = 0

            timestamp = isodatetime_filename()
            to_filename = f"{directory}/{basename}.{timestamp}{suffix}"

            await self.disconnect()
            try:
                await self.__create_directory(to_filename)
                shutil.copy2(self.__database_name, to_filename)
                logger.debug(f"backed up to {to_filename}")
            except Exception:
                raise RuntimeError(
                    f"copy {self.__database_name} to {to_filename} failed"
                )
            finally:
                await self.connect()

    # ----------------------------------------------------------------------------------------
    async def restore(self, nth):
        """
        Restore database from timestamped location.
        """

        async with self.__backup_restore_lock:
            directory = self.__backup_directory
            if directory is None:
                raise RuntimeError("no backup directory supplied in confirmation")

            basename, suffix = os.path.splitext(os.path.basename(self.__database_name))

            filenames = glob.glob(f"{directory}/{basename}.*{suffix}")

            filenames.sort(reverse=True)

            if nth >= len(filenames):
                raise RuntimeError(
                    f"restoration index {nth} is more than available {len(filenames)}"
                )

            from_filename = filenames[nth]

            await self.disconnect()
            try:
                shutil.copy2(from_filename, self.__database_name)
                logger.debug(
                    f"restored nth {nth} out of {len(filenames)} from {from_filename}"
                )
            except Exception:
                raise RuntimeError(
                    f"copy {from_filename} to {self.__database_name} failed"
                )
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
