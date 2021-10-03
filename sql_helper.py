#!/usr/bin/python
#
# Copyright (C) 2021 Steve Campbell
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Classes to provide database independent utility methods to both simplify
code and enable unit testing of functions against an in memory SQLite
database.

Databases must be compliant to PEP 249 (Python Database API Specification
v2.0).

Example::

    from sql_helper_mysql import MysqlHelper
    db = MysqlHelper(url="mysql://test:test@127.0.0.1/TestDb")
    db.insert("TestTab", {"Id": 1, "Col1": "a", "Col2": "b"})
    db.update("TestTab", {"Col1": "c"}, {"Id": 1})
    col1, col2 = db.row("SELECT Col1, Col2 FROM TestTab WHERE Id=?", bind=[1])
    for row in db.rows("SELECT * FROM TestTab"):
        print("Found col1 %s" % row["Col1"])

"""

__author__ = "Steve Campbell"

import logging
import re
import sqlite3
import time
from abc import ABC, abstractmethod
from typing import List, Optional, Tuple, Union


class SqlHelper(ABC):

    def __init__(self, url: Optional[str] = None, con=None):
        """ Initialize.
        Depending on the subclass, we can be provided with
        an open database connection, or a URL to lazy connect.
        Example URLS:
          mysql://username:password@host:port/database_name
          sqlite://:memory:

        :param url: Database URL.
        :param con: Database connection handle.
        :return: None
        """
        self.url = url
        self._con = con
        self._last_sql = None
        self._last_bind = None

    @property
    def con(self):
        """ Do a lazy connect to the database
        :return: Database connection handle
        """
        if not self._con:
            self._con = self.connect()
        return self._con

    @con.setter
    def con(self, con):
        """ Inject the database connection post-initialisation
        :param con: The database connection handle to be used.
        :return: None
        """
        self._con = con

    @abstractmethod
    def connect(self):
        """ Connect to the database specified in the url attribute.
        :return: Database connection handle
        """

    @abstractmethod
    def execute(self, sql: str, bind: Optional[list] = None, options: Optional[dict] = None):
        """ Prepare, bind and execute a statement.
        :param sql: The SQL statement to execute.
        :param bind: List of parameters to be bound into the statement.
        :param options: Any query specific options.
        Options include "RowType" - return each row as a "Tuple" (default), or
        "Dict"
        :return: The cursor.
        """
        self._last_sql = sql
        self._last_bind = bind or []

    def last_sql(self) -> str:
        """ :return: A string of the last SQl and bind parameters for logging
         or debugging.
         """
        return "SQL: %s, Bind: (%s)" % (self._last_sql, ", ".join(map(str, self._last_bind)))

    def row(self, sql: str, bind: Optional[list] = None, options: Optional[dict] = None)\
            -> Optional[Union[tuple, dict]]:
        """ Execute some SQL and return the row as a tuple.
        Return None if we don't get a result.

        :param sql: The SQL statement to execute.
        :param bind: List of parameters to be bound into the statement.
        :param options: Set "RowType": "Dict" for the row to be returned as a
        dict instead of a tuple.
        :raises RuntimeError: If multiple rows are found.
        :return: None, a tuple of the row, or a dict if specified.
        """
        cur = self.execute(sql, bind, options)
        row = cur.fetchone()
        row2 = cur.fetchone()
        if row2:
            raise RuntimeError("Multiple rows returned from %s" % self.last_sql())

        return row

    def value(self, sql: str, bind: Optional[list] = None, options: Optional[dict] = None):
        """ Execute some SQL and return the value of the
        first field of the first row.
        Return None if we don't get a result.

        :param sql: The SQL statement to execute.
        :param bind: List of parameters to be bound into the statement.
        :param options: currently unused
        :raises RuntimeError: If multiple rows are found.
        :return: The first field of the first row.
        """
        row = self.row(sql, bind, options)
        if row:
            return row[0]

    def rows(self, sql: str, bind: Optional[list] = None, options: Optional[dict] = None) -> Tuple[dict]:
        """ Execute SQL and return a list of dicts

        :param sql: The SQL statement to execute.
        :param bind: List of parameters to be bound into the statement.
        :param options: currently unused
        """
        options = options or {}
        options2 = options.copy()
        options2["RowType"] = options.get("RowType", "Dict")
        cur = self.execute(sql, bind, options2)
        return tuple(cur.fetchall())

    def column(self, sql: str, bind: Optional[list] = None, options: Optional[dict] = None) -> List:
        """ Execute SQL and return a list of the first field
         in each row.
        :param sql: The SQL statement to execute.
        :param bind: List of parameters to be bound into the statement.
        :param options: currently unused
        :return: List of the first field in each row.
        """
        cur = self.execute(sql, bind, options)
        column = []
        while True:
            row = cur.fetchone()
            if not row:
                break
            column.append(row[0])

        return column

    def insert(self, table: str, attributes: dict) -> None:
        """ Insert a row of data into a table.

        :param table: The table to insert the row into.
        :param attributes: Dict of Field: Value for the row.
        """
        fields = "`" + "`,`".join(attributes.keys()) + "`"
        placeholder_list = "?" * len(attributes)
        placeholders = ",".join(placeholder_list)
        sql = f"INSERT INTO {table}({fields}) VALUES({placeholders})"
        self.execute(sql, bind=list(attributes.values()))

    def update(self, table: str, attributes: dict, filters: dict) -> None:
        """ Update a table.

        :param table: The table to be updated.
        :param attributes: Fields to be updated -
            {Field1: value1, field2: value2, ...}.
        :param filters: Dict of {Field: Value} to filter the rows which are
            updated. Filters must be specified, but can be empty.
        :return: None. We would return the rowcount, but SQLite returns a
            different rowcount to other database engines.
        """

        # Not changing anything? Just return
        if len(attributes) == 0:
            return

        set_str = "SET `" + "`=?, `".join(attributes.keys()) + "`=?"
        bind_values = list(attributes.values())

        where_str = ""
        if len(filters) > 0:
            where_str = " WHERE `" + "`=? AND `".join(filters.keys()) + "`=?"
            bind_values = bind_values + list(filters.values())

        sql = f"UPDATE `{table}` {set_str}{where_str}"
        self.execute(sql, bind=bind_values)


class SqliteHelper(SqlHelper):
    """ Extend the SqlHelper class for SQLite databases """

    def connect(self) -> sqlite3.Connection:
        """ Connect to the sqlite3 database specified in the url attribute """
        # Normal URL parsing won't cope with :memory:
        database = re.sub(r"sqlite\d?://", "", self.url)
        database = re.sub(r"/.*", "", database)
        con = sqlite3.connect(database)
        return con

    def execute(self, sql: str, bind: Optional[list] = None, options: Optional[dict] = None) -> sqlite3.Cursor:
        """ Prepare, bind and execute a statement.

        See :py:meth:`.SqlHelper.execute`

        :param sql: The SQL statement to execute.
        :param bind: List of parameters to be bound into the statement.
        :param options: Any query specific options.
        Options include "RowType" - return each row as a "Tuple" (default), or
        "Dict"
        :return: The cursor.
        """
        bind = bind or []
        options = options or {}
        super().execute(sql, bind, options)
        saved_row_factory = self.con.row_factory
        if options.get("RowType", "Tuple") == "Dict":
            self.con.row_factory = sqlite_dict_factory

        cur = self.con.cursor()
        sql = self.sql_to_sqlite3(sql)
        logging.info("Executing %s", self.last_sql())
        cur.execute(sql, bind)
        self.con.row_factory = saved_row_factory

        return cur

    def sql_to_sqlite3(self, sql: str) -> str:
        """ Make a few conversions from other formats to SQLite format.
        This is not intended to handle everything, just a few common cases.
        We currently handle:
          %s to ?
          unix_timestamp()
        """
        unix_time = int(time.time())
        sql = re.sub("%s", "?", sql)
        sql = re.sub(r"unix_timestamp\(\)", str(unix_time), sql, flags=re.IGNORECASE)
        return sql


def sqlite_dict_factory(cursor, row):
    """ See `SQLite Docs
    <https://docs.python.org/3/library/sqlite3.html>`_
    """
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d
