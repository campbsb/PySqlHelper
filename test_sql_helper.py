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

__author__ = "Steve Campbell"

# This test suite runs for both Sqlite and MySQL.
# Run it with 'pytest test_sql_helper.py'

# To run it for MySQL:
# * Start up a MySQl/MariaDB database with an empty database schema
# * Uncomment the MysqlHelper import line
# * Update the db() fixture with an appropriate connection URL
# * Run pytest as above

import re
import sqlite3

import pytest

from sql_helper import SqlHelper, SqliteHelper


# from sql_helper_mysql import MysqlHelper

@pytest.fixture
def db() -> SqlHelper:
    # return MysqlHelper(url="mysql://test:test@127.0.0.1/TestDb")
    return SqliteHelper(url="sqlite://:memory:/")


@pytest.fixture
def db2(db) -> SqlHelper:
    db.execute("DROP TABLE IF EXISTS Test ")
    db.execute("CREATE TABLE Test(Id INTEGER, Value TEXT)")
    db.execute("INSERT INTO Test(Id, Value) VALUES (1, 'a'), (2, 'b')")
    return db


def test_connect(db):
    # Matches both pymysql.connections.Connection and sqlite3.Connection
    assert re.search("Connection", str(type(db.connect())))


def test_execute_returns_cursor(db):
    type_str = str(type(db.execute("SELECT 1")))
    assert re.search(r"(Cursor|MysqlHelper)", type_str)


def test_execute_statement_was_executed(db):
    cur: sqlite3.Cursor = db.execute("SELECT 1")
    assert list(cur.fetchone())[0] == 1


def test_execute_returns_addressable_by_name(db):
    row = db.execute("SELECT 1 AS A", options={"RowType": "Dict"}).fetchone()
    assert row["A"] == 1


def test_execute_bind_with_question_mark(db):
    row = db.execute("SELECT ?", [2]).fetchone()
    assert row[0] == 2


def test_execute_bind_with_percent(db):
    row = db.execute("SELECT %s", [2]).fetchone()
    assert row[0] == 2


def test_row_returns_none(db2):
    row = db2.row("SELECT Id,Value FROM Test WHERE Id=99")
    assert row is None


def test_row_returns_tuple(db2):
    row = db2.row("SELECT Id,Value FROM Test WHERE Id=?", [1])
    assert row == (1, 'a')


def test_row_returns_tuple2(db2):
    row_id, value = db2.row("SELECT Id,Value FROM Test WHERE Id=?", [1])
    assert row_id == 1
    assert value == "a"


def test_row_returns_exception(db2):
    with pytest.raises(RuntimeError, match=r"Multiple rows"):
        db2.row("SELECT * FROM Test")


def test_value(db2):
    assert db2.value("SELECT Value from Test where Id=?", [1]) == "a"


def test_rows(db2):
    assert db2.rows("SELECT * FROM Test") == (
        {"Id": 1, "Value": "a"}, {"Id": 2, "Value": "b"}
    )


def test_column(db2):
    assert db2.column("SELECT Value FROM Test") == ["a", "b"]


def test_insert(db2):
    db2.insert("Test", {"Id": 3, "Value": "c"})
    assert db2.value("SELECT COUNT(*) FROM Test") == 3


@pytest.mark.parametrize(
    "attributes, filters, exp_state, description", [
        ({"Value": "d"}, {}, (
                {"Id": 1, "Value": "d"}, {"Id": 2, "Value": "d"}
        ), "Update every row, no filter"),
        ({"Value": "a"}, {}, (
                {"Id": 1, "Value": "a"}, {"Id": 2, "Value": "a"}
        ), "Update where one row is unaffected"),
        ({"Value": "a"}, {"Id": 1}, (
                {"Id": 1, "Value": "a"}, {"Id": 2, "Value": "b"}
        ), "Update filtering on an unaffected row"),
        ({"Value": "a"}, {"Id": 2}, (
                {"Id": 1, "Value": "a"}, {"Id": 2, "Value": "a"}
        ), "Update filtering on an affected row"),
        ({"Value": "d"}, {"Id": 99}, (
                {"Id": 1, "Value": "a"}, {"Id": 2, "Value": "b"}
        ), "Update filtering out all rows"),
        ({}, {}, (
                {"Id": 1, "Value": "a"}, {"Id": 2, "Value": "b"}
        ), "Make an empty update"),
    ]
)
def test_update(db2, attributes, filters, exp_state, description):
    db2.update("Test", attributes=attributes, filters=filters)
    assert db2.rows("SELECT * FROM Test") == exp_state
