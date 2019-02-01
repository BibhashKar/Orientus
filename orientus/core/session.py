import inspect
from abc import ABC, abstractmethod
from collections import OrderedDict
from enum import Enum
from typing import List, Type

from pyorient import OrientRecord, PyOrientCommandException

from orientus.core.datatypes import Clause, RawType
from orientus.core.db import OrientUsDB
from orientus.core.domain import ORecord, OVertex, OEdge


class AbstractSession(ABC):

    def __init__(self, db: OrientUsDB):
        self.db = db
        self.debug = db.debug

        self.command_history = []

    def __enter__(self):
        self.connection = self.db.acquire_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.release_connection(self.connection)

    @abstractmethod
    def command(self, statement: str, record: ORecord = None):
        pass

    @abstractmethod
    def raw_query(self, query: str, limit: int = -1) -> str:
        if limit > -1 and ' limit ' not in query:
            query = '%s limit %s' % (query, limit)

        return query

    def create_class(self, clz) -> bool:

        # TODO: clasees now only extends V, E not custom vertex classes (e.g. CREATE CLASS Car EXTENDS Vehicle)
        # for help https://orientdb.com/docs/2.1.x/SQL-Create-Class.html

        if issubclass(clz, OVertex):
            clz_create_cmd = 'CREATE CLASS %s IF NOT EXISTS EXTENDS V' % clz.__name__
        elif issubclass(clz, OEdge):
            clz_create_cmd = 'CREATE CLASS %s IF NOT EXISTS EXTENDS E' % clz.__name__
        else:
            clz_create_cmd = 'CREATE CLASS %s IF NOT EXISTS' % clz.__name__

        if self.debug: print(clz_create_cmd)

        self.command(clz_create_cmd)

        attributes = inspect.getmembers(clz, lambda a: not (inspect.isroutine(a)))
        clz_attrs = [a for a in attributes if not (a[0].startswith('__') and a[0].endswith('__'))]

        unique_indices = []

        for (name, datatype) in clz_attrs:
            if datatype.__class__ == RawType:
                continue

            if isinstance(datatype, RawType):

                prop_cmd = "CREATE PROPERTY %s.%s %s" % (
                    clz.__name__,
                    datatype.name,
                    datatype.__class__.__name__[1:].upper()
                )

                constraints = []

                # TODO: add all types of constraints schema for ORecord, OVertex, OEdge
                # for help https://orientdb.com/docs/2.2.x/SQL-Create-Property.html, https://orientdb.com/docs/3.0.x/gettingstarted/Tutorial-Classes.html

                if datatype.min is not None:
                    constraints.append("MIN %s" % (datatype.min))

                if datatype.max is not None:
                    constraints.append("MAX %s" % (datatype.max))

                if datatype.mandatory:
                    constraints.append("MANDATORY TRUE")

                if datatype.unique:
                    unique_indices.append("%s.%s" % (clz.__name__, datatype.name))

                if len(constraints) > 0:
                    prop_cmd = "%s (%s)" % (prop_cmd, ",".join(constraints))

                if self.debug:
                    print(prop_cmd)

                self.command(prop_cmd)

        for index in unique_indices:
            index_cmd = "CREATE INDEX %s UNIQUE" % (index)

            if self.debug:
                print(index_cmd)

            self.command(index_cmd)

        if self.debug:
            print()

        return True

    def __save_edge(self, frm: OVertex, to: OVertex, edge: OEdge) -> bool:
        frm_id = self._get_id(frm)
        to_id = self._get_id(to)

        assert bool(frm_id)
        assert bool(to_id)

        create_cmd = "create edge %s from %s to %s" % (edge.class_name(), frm_id, to_id)
        value_str = self._fields_to_str(edge)
        if value_str:
            create_cmd += ' set ' + value_str

        self.command(create_cmd, edge)

        return True

    def save(self, record: ORecord) -> bool:
        clz_name = record.class_name()

        if isinstance(record, OEdge):
            return self.__save_edge(record._from_vertex, record._to_vertex, record)

        insert_cmd = "insert into %s set " % (clz_name) + self._fields_to_str(record)

        self.command(insert_cmd, record)

        return True

    @abstractmethod
    def _get_id(self, record: ORecord) -> str:
        pass

    def _fields_to_str(self, record, delimiter=',') -> str:
        values = []
        for field, value in record.__dict__.items():
            if field in ['_batch_id', '_rid', '_version', '_from_vertex',
                         '_to_vertex']:  # TODO: business domain object can have these field name?
                continue
            modified_val = "'%s'" % value.replace("'", "\\'") if type(value) == str else value
            values.append("%s = %s" % (field, modified_val))
        return (' %s ' % delimiter).join(values)

    def _data_to_ORecord(self, data: OrientRecord, cls) -> ORecord:
        obj = type(cls.__name__, (cls,), data.__dict__['_OrientRecord__o_storage'])
        obj._rid = data._rid
        obj._version = data._version

        return obj


class GraphFunction:

    def __init__(self, edge_class=None):
        pass


def prop(*args):
    pass


class Graph(GraphFunction):

    def __init__(self):
        super().__init__()

        self.__sql = []

        self.__vertex_on = False
        self.__vertex_dict = {}

    def vertex(self, vertex: Type[OVertex], alias: str) -> 'Graph':
        self.__vertex_on = True
        self.__vertex_dict = {'class': vertex.___vertex_name__, 'as': alias}
        return self

    def where(self, clause: Clause) -> 'Graph':
        if self.__vertex_on:
            self.__vertex_dict['where'] = str(clause)
        return self

    def while_(self, clause: Clause) -> 'Graph':
        if self.__vertex_on:
            self.__vertex_dict['while'] = str(clause)
        return self

    def outE(self, edge: Type[OEdge]) -> 'Graph':
        self.__close_vertex()
        self.__sql.append(".%s(%s)" % ("out", edge.__edge_name__))
        return self

    def inE(self, edge: Type[OEdge]) -> 'Graph':
        self.__close_vertex()
        self.__sql.append(".%s(%s)" % ('in', edge.__edge_name__))
        return self

    def bothE(self, edge: Type[OEdge]) -> 'Graph':
        self.__close_vertex()
        self.__sql.append(".%s(%s)" % ('both', edge))
        return self

    def not_(self) -> 'Graph':
        self.__close_vertex()
        self.__sql.append(", NOT")
        return self

    def return_(self, clause: str, distinct=False, alias=None) -> 'Graph':
        self.__close_vertex()

        return_str = "RETURN"

        if distinct:
            return_str += " DISTINCT"

        return_str += " %s" % (clause)

        if alias:
            return_str += " %s" % (alias)

        self.__sql.append(return_str)

        return self

    def group_by(self, clause: str) -> 'Graph':
        self.__close_vertex()
        if clause:
            self.__sql.append("GROUP BY %s" % (clause))
        return self

    def order_by(self, clause: str) -> 'Graph':
        self.__close_vertex()
        if clause:
            self.__sql.append("ORDER BY %s" % (clause))
        return self

    def skip(self, number: int) -> 'Graph':
        self.__close_vertex()
        self.__sql.append("SKIP %s" % (number))
        return self

    def limit(self, number: int) -> 'Graph':
        self.__close_vertex()
        self.__sql.append("LIMIT %s" % (number))
        return self

    def done(self) -> str:
        self.__close_vertex()
        return "MATCH\n" + "\n".join(self.__sql)

    def __close_vertex(self):
        if self.__vertex_on:

            self.__vertex_on = False

            class_str = "as: %s" % (self.__vertex_dict['as'])

            if self.__vertex_dict.get('class') != None:
                class_str += ", class: %s" % (self.__vertex_dict['class'])

            if self.__vertex_dict.get('where') != None:
                class_str += ", where: (%s)" % (self.__vertex_dict.get('where'))

            if self.__vertex_dict.get('while') != None:
                class_str += ", while: (%s)" % (self.__vertex_dict.get('while'))

            self.__sql.append("{%s}" % (class_str))

            self.__vertex_dict = {}


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

    def _done(self):
        # print("\n".join(self.sql))
        return "\n".join(self.sql)


class Session(AbstractSession):

    def command(self, statement, record: ORecord = None) -> List[OrientRecord]:
        if self.debug: print('Command:', statement)
        try:
            results = self.connection.command(statement)

            if self.debug: print('Result Count:', len(results))

            if record is not None and len(results) > 0:
                record._rid = results[0]._rid
                record._version = results[0]._version

                if self.debug: print('rid', results[0]._rid)

        except PyOrientCommandException:
            return []

        # TODO: return type should be OrientRecord or our custom object

        return results

    def raw_query(self, query: str, limit: int = -1) -> List[OrientRecord]:
        query = super().raw_query(query, limit)

        results = self.command(query)

        return results

    def query(self, qry: Query) -> List[OrientRecord]:
        return self.raw_query(qry._done())

    def update(self, record: ORecord, upsert=True) -> bool:
        update_cmd = "update %s set %s %s where %s" % (
            record.class_name(),
            self._fields_to_str(record),
            'upsert' if upsert else '',
            self._fields_to_str(record, delimiter='AND')
        )

        self.command(update_cmd, record)

        return True

    def update_by_id(self, record: ORecord) -> bool:
        update_cmd = "update %s set %s where @rid = '%s'" % (
            record.class_name(),
            self._fields_to_str(record),
            self._get_id(record)
        )

        self.command(update_cmd, record)

        return True

    def delete(self, record: ORecord) -> bool:
        id = self._get_id(record)

        if isinstance(record, OVertex):
            delete_cmd = "delete vertex %s" % id
        elif isinstance(record, OEdge):
            delete_cmd = "delete edge %s" % id
        else:
            delete_cmd = "delete from %s where @rid = %s" % (record.class_name(), id)

        self.command(delete_cmd, record)

        return True

    def _get_id(self, record: ORecord) -> str:
        return record._rid

    def match(self, graph: Graph) -> List[OrientRecord]:
        return self.command(graph.done())


#     TODO: all operations are returning TRUE...need to return false when opration became unsuccessful


class BatchQueryBuilder:

    def __init__(self):
        self.index = 0
        self.record_list: List[ORecord] = []
        self.query_dict = OrderedDict()

    def add(self, statement: str, record: ORecord = None):
        self.index += 1
        idx_str = str(self.index)

        if record is None:
            self.query_dict['qry' + idx_str] = statement
        else:
            if record._batch_id is None:
                record._batch_id = record.class_name() + idx_str
                self.query_dict[record._batch_id] = statement

                self.record_list.append(record)

    # TODO: think about batch return data
    def finalize(self) -> str:
        for r in self.record_list:
            r._batch_id = None

        result = ["let %s = %s" % (variable, query) for variable, query in self.query_dict.items()]

        return ";\n".join(["begin", ";\n".join(result), "commit retry 10;"])


class BatchSession(AbstractSession):

    def __init__(self, db: OrientUsDB):
        super().__init__(db)

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)

        if self.query_builder is not None:
            raise ValueError("batch did not end properly")

    def start_batch(self):
        self.query_builder = BatchQueryBuilder()

    def end_batch(self):
        # if self.debug: print(self.query_builder.finalize())

        record_classes = {}
        for record in self.query_builder.record_list:
            if not isinstance(record, OEdge) and record.class_name() not in record_classes.keys():
                record_classes[record.class_name()] = record

        # print(record_classes)
        batch_query = self.query_builder.finalize()
        if self.debug: print(batch_query)
        result = self.connection.batch(batch_query)
        # print('Batch result:', result)

        self.query_builder = None

    def command(self, statement: str, record: ORecord = None):
        if self.debug: print('Command:', statement)
        self.query_builder.add(statement, record)

    def raw_query(self, query: str, limit: int = -1) -> bool:
        query = super().raw_query(query, limit)

        print(query)
        self.query_builder.add(query)

        return True

    def save(self, record: ORecord) -> bool:
        return super().save(record)

    def _get_id(self, record: ORecord) -> str:
        return "$" + record._batch_id
