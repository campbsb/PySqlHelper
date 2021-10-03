# PySqlHelper
Classes to provide database independent utility methods to
* Simplify frequent types of SELECT calls -
  * value(sql, bind) - extract a single value from the database
  * row(sql, bind) - extract a single row from the database as a tuple
  * rows(sql, bind) - extract a set of rows as a tuple of dicts
* Simplify common complex calls -
  * insert(sql, attributes)
  * update(sql, attributes, filters)
  
This provides a database access abstraction layer allowing for function
unit tests to test against an in memory SQLite database, despite the production
database having a different backend engine.

Database Engines must be compliant to PEP 249 (Python Database API Specification
v2.0). We currently provide classes for:
* MySQL/MariaDB
* SQLite

Example:
```
    from sql_helper_mysql import MysqlHelper
    db = MysqlHelper(url="mysql://test:test@127.0.0.1/TestDb")
    db.insert("TestTab", {"Id": 1, "Col1": "a", "Col2": "b"})
    db.update("TestTab", {"Col1": "c"}, {"Id": 1})
    col1, col2 = db.row("SELECT Col1, Col2 FROM TestTab WHERE Id=?", bind=[1])
    for row in db.rows("SELECT * FROM TestTab"):
        print("Found col1 %s" % row["Col1"])
```

Code tested against Python3.

## Using in Unit tests
Here is how to inject an Sqlite backed database into your object
in unit tests:
```
import pytest
from sql_helper import SqliteHelper
 
@pytest.fixture()
def db():
    fresh_db = SqliteHelper(url="sqlite://:memory:/")
    # Do setup
    return fresh_db


@pytest.fixture()
def my_object(db):
    return MyObject(db=db)


def test_my_method(my_object):
    assert my_object.my_method() == "result"
```