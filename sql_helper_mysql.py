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

""" Extend SqlHelper for interacting with MySQL/MariaDB databases.
"""

__author__ = "Steve Campbell"

import logging
import re
from typing import Optional
from urllib.parse import urlparse

import pymysql

from sql_helper import SqlHelper


class MysqlHelper(SqlHelper):
    """ Extend the SqlHelper class for SQLite databases """

    def connect(self) -> pymysql.Connection:
        """ Connect to the MySQL database specified in the url attribute """
        parsed_url = urlparse(self.url)
        database = re.sub(r"^/", "", parsed_url.path)
        con = pymysql.connect(
            user=parsed_url.username,
            password=parsed_url.password,
            host=parsed_url.hostname,
            port=parsed_url.port or 3306,
            database=database
        )
        return con

    def execute(self, sql: str, bind: Optional[list] = None, options: Optional[dict] = None) -> pymysql.cursors.Cursor:
        """ Create a cursor and execute it
        :param sql: The SQL to run. Bind placeholders can be %s or ?'
        :param bind: List of bind parameters.
        :param options: "RowType": "Tuple" (default), or "Dict"
        """
        bind = bind or []
        options = options or {}
        super().execute(sql, bind, options)
        if options.get("RowType", None) == "Dict":
            cur = self.con.cursor(pymysql.cursors.DictCursor)
        else:
            cur = self.con.cursor()

        sql = self.sql_to_mysql(sql)
        logging.info("Executing %s", self.last_sql())
        cur.execute(sql, bind)
        #        self.con.row_factory = saved_row_factory

        return cur

    def sql_to_mysql(self, sql: str) -> str:
        """ Make a few conversions from other formats to MySQL format.
        This is not intended to handle everything, just a few common cases.
        We currently handle:
          ? to %s
        """
        sql = re.sub(r"\?", "%s", sql)
        return sql
