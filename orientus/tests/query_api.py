# TODO: implement select
"""
SELECT [ <Projections> ] [ FROM <Target> [ LET <Assignment>* ] ]
    [ WHERE <Condition>* ]
    [ GROUP BY <Field>* ]
    [ ORDER BY <Fields>* [ ASC|DESC ] * ]
    [ UNWIND <Field>* ]
    [ SKIP <SkipRecords> ]
    [ LIMIT <MaxRecords> ]
    [ FETCHPLAN <FetchPlan> ]
    [ TIMEOUT <Timeout> [ <STRATEGY> ]  RETURN or EXCEPTION
    [ LOCK default|record ]
    [ PARALLEL ]
    [ NOCACHE ]
"""
from enum import Enum
from typing import Type

from orientus.core.datatypes import Clause
from orientus.core.domain import OVertex
from orientus.tests.data import Token


class Order(Enum):
    ASC = "ASC"
    DESC = "DESC"


class Timeout(Enum):
    RETURN = "RETURN"
    EXCEPTION = "EXCEPTION"


class Lock(Enum):
    # DEFAULT = "DEFAULT"
    RECORD = "RECORD"


class Query:

    def __init__(self):
        "Constructor for query"
        self.sql = []

    def select(self, *fields):
        _fields = ""
        if fields is not None:
            _fields = ", ".join([f.name for f in fields])

        _sql = "SELECT %s" % (_fields)

        self.sql.append(_sql)

        return self

    def from_(self, record_cls: Type[OVertex]):
        _sql = "FROM %s" % (record_cls.___vertex_name__)

        self.sql.append(_sql)

        return self

    def where(self, clause: Clause):
        _sql = "WHERE %s" % (str(clause))

        self.sql.append(_sql)

        return self

    def group_by(self, *fields):
        if fields is not None:
            _fields = ", ".join([f.name for f in fields])
            _sql = "GROUP BY %s" % (_fields)

            self.sql.append(_sql)

        return self

    def order_by(self, *fields, order=Order.ASC):
        # TODO: add order to all individual fields or common?
        if fields is not None:
            _fields = ", ".join([f.name for f in fields])
            _sql = "ORDER BY %s %s" % (_fields, order.value)

            self.sql.append(_sql)

        return self

    def unwind(self, *fields):
        if fields is not None:
            _fields = ", ".join([f.name for f in fields])
            _sql = "UNWIND %s" % (_fields)

            self.sql.append(_sql)

        return self

    def skip(self, number: int = 0):
        _sql = "SKIP %s" % (number)

        self.sql.append(_sql)

        return self

    def limit(self, number: int = 0):
        _sql = "LIMIT %s" % (number)

        self.sql.append(_sql)

        return self

    def fetchplan(self):
        # Know nothing yet
        return self

    def timeout(self, millis: int, strategy: Timeout = Timeout.EXCEPTION):
        assert millis > 0

        _sql = "TIMEOUT %s %s" % (millis, strategy.value)

        self.sql.append(_sql)

        return self

    def lock(self, lock_type: Lock = Lock.RECORD):
        _sql = "LOCK %s" % (lock_type.value)

        self.sql.append(_sql)

        return self

    def parallel(self):
        _sql = "PARALLEL"

        self.sql.append(_sql)

        return self

    def cache(self):
        _sql = "NOCACHE"

        self.sql.append(_sql)

        return self

    def done(self):
        print("\n".join(self.sql))


if __name__ == '__main__':
    # TODO: think about selecting columns from multiple classes

    Query().select(Token.text, Token.new_text) \
        .from_(Token) \
        .where(Token.text == 'to') \
        .group_by(Token.text) \
        .order_by(Token.text, order=Order.DESC) \
        .unwind(Token.text) \
        .skip(number=3) \
        .limit(number=10) \
        .fetchplan() \
        .timeout(100) \
        .lock() \
        .parallel() \
        .cache() \
        .done()
