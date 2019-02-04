from enum import Enum
from typing import Type

from orientus.core.datatypes import OString
from orientus.core.domain import OVertex


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

    # TODO: add support for edge and record class too

    def __init__(self, record_cls: Type[OVertex]):
        "Constructor for query"
        self.sql = []

        self.record_cls = record_cls
        if record_cls is not None:
            self.select().from_(record_cls)

    def select(self, *fields):
        _fields = ""
        if fields is not None:
            _fields = ", ".join([f.name for f in fields])
            _sql = "SELECT %s" % (_fields)
        else:
            _sql = "SELECT"

        self.sql.append(_sql)

        return self

    def from_(self, record_cls: Type[OVertex]):
        _sql = "FROM %s" % (record_cls.element_name())

        self.sql.append(_sql)

        return self

    def where(self, clause):  # TODO: clause accepts OString and Clause type as param

        if isinstance(clause, OString):
            search_term = clause.name
        else:
            search_term = str(clause)

        _sql = "WHERE %s" % (search_term)

        self.sql.append(_sql)

        return self

    def like(self, match: str):
        _sql = "LIKE '%s'" % (match)

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

    def _done(self):
        # print("\n".join(self.sql))
        return "\n".join(self.sql)