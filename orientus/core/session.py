import inspect
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import List

from pyorient import OrientRecord, PyOrientCommandException, PyOrientORecordDuplicatedException

from orientus.core.datatypes import RawType
from orientus.core.db import OrientUsDB
from orientus.core.domain import ORecord, OVertex, OEdge
from orientus.core.match import Graph
from orientus.core.query import Query
from orientus.core.utils import to_datatype_obj


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
            assert clz.___vertex_name__ is not None
            clz_create_cmd = 'CREATE CLASS %s IF NOT EXISTS EXTENDS V' % clz.element_name()
        elif issubclass(clz, OEdge):
            assert clz.__edge_name__ is not None
            clz_create_cmd = 'CREATE CLASS %s IF NOT EXISTS EXTENDS E' % clz.element_name()
        else:
            assert clz.__record_name__ is not None
            clz_create_cmd = 'CREATE CLASS %s IF NOT EXISTS' % clz.element_name()

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
                    clz.element_name(),
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
                    unique_indices.append("%s.%s" % (clz.element_name(), datatype.name))

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

    def __save_edge(self, frm: OVertex, to: OVertex, edge: OEdge) -> OEdge:
        frm_id = self._get_id(frm)
        to_id = self._get_id(to)

        assert bool(frm_id)
        assert bool(to_id)

        create_cmd = "create edge %s from %s to %s" % (edge.element_name(), frm_id, to_id)
        value_str = self._fields_to_str(edge)
        if value_str:
            create_cmd += ' set ' + value_str

        self.command(create_cmd, edge)

        return edge

    def save(self, record: ORecord) -> ORecord:
        clz_name = record.element_name()

        if isinstance(record, OEdge):
            return self.__save_edge(record._from_vertex, record._to_vertex, record)

        insert_cmd = "insert into %s set " % (clz_name) + self._fields_to_str(record)

        self.command(insert_cmd, record)

        return record

    @abstractmethod
    def _get_id(self, record: ORecord) -> str:
        pass

    def _fields_to_str(self, record, delimiter=',') -> str:
        values = []
        items = record.__dict__.items()

        if len(items) == 0:
            raise ValueError(record.__class__.__name__ + " has no properties.")

        for field, value in items:
            if field in ['_batch_id', '_rid', '_version', '_from_vertex',
                         '_to_vertex']:  # TODO: business domain object can have these field name?
                continue

            modified_val = "'%s'" % value.replace("'", "\\'") if type(value) == str else value

            values.append("%s = %s" % (field, modified_val))

        return (' %s ' % delimiter).join(values)


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

        except PyOrientCommandException as e:
            print('error occured during executing command', e)
            return []

        # TODO: return type should be OrientRecord or our custom object

        return results

    def raw_query(self, query: str, limit: int = -1) -> List[OrientRecord]:
        query = super().raw_query(query, limit)

        results = self.command(query)

        return results

    def query(self, qry: Query) -> List:
        results = self.raw_query(qry._done())
        return to_datatype_obj(qry.record_cls, results)

    def save_if_not_exists(self, record: ORecord) -> ORecord:
        try:
            self.save(record)
            return record
        except PyOrientORecordDuplicatedException:
            # print('Duplicate Exception in save_or_fetch()')
            # print('record', record)
            result = self.command(
                "SELECT FROM %s WHERE %s" % (record.element_name(), self._fields_to_str(record, delimiter='AND')))
            cast_result = to_datatype_obj(record.__class__, result)
            # print('result', result)
            # print('result rid', cast_result[0]._rid)
            return cast_result[0]

    def update(self, record: ORecord, upsert=True) -> bool:
        update_cmd = "update %s set %s %s where %s" % (
            record.element_name(),
            self._fields_to_str(record),
            'upsert' if upsert else '',
            self._fields_to_str(record, delimiter='AND')
        )

        self.command(update_cmd, record)

        return True

    def update_by_id(self, record: ORecord) -> bool:
        update_cmd = "update %s set %s where @rid = '%s'" % (
            record.element_name(),
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
            delete_cmd = "delete from %s where @rid = %s" % (record.element_name(), id)

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
                record._batch_id = record.element_name() + idx_str
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
            if not isinstance(record, OEdge) and record.element_name() not in record_classes.keys():
                record_classes[record.element_name()] = record

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

        if self.debug: print(query)
        self.query_builder.add(query)

        return True

    def save(self, record: ORecord) -> bool:
        return super().save(record)

    def _get_id(self, record: ORecord) -> str:
        return "$" + record._batch_id
