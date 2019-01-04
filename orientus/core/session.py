from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import List

from pyorient import OrientRecord, PyOrientCommandException

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
    def query(self, query: str, limit: int = -1) -> str:
        if limit > -1 and ' limit ' not in query:
            query = '%s limit %s' % (query, limit)

        return query

    def create_class(self, clz_name, record) -> bool:
        if isinstance(record, OVertex):
            clz_create_cmd = 'create class %s if not exists extends V' % clz_name
        else:
            clz_create_cmd = 'create class %s if not exists' % clz_name

        # if clz_create_cmd in self.command_history:
        #     return False
        # else:
        #     self.command_history.append(clz_create_cmd)

        if self.debug: print(clz_create_cmd)

        self.connection.command(clz_create_cmd)

        return True

    def add_edge(self, frm: OVertex, to: OVertex, edge: OEdge) -> bool:
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

    def save(self, record: ORecord, create_class=True) -> bool:
        clz_name = record.class_name()

        if isinstance(record, OEdge):
            return self.add_edge(record._from_vertex, record._to_vertex, record)

        if create_class: self.create_class(clz_name, record)

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

        return results

    def query(self, query: str, limit: int = -1) -> List[OrientRecord]:
        query = super().query(query, limit)

        results = self.command(query)

        return results

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
        for cls, record in record_classes.items():
            self.create_class(cls, record)

        batch_query = self.query_builder.finalize()
        if self.debug: print(batch_query)
        result = self.connection.batch(batch_query)
        # print('Batch result:', result)

        self.query_builder = None

    def command(self, statement: str, record: ORecord = None):
        if self.debug: print('Command:', statement)
        self.query_builder.add(statement, record)

    def query(self, query: str, limit: int = -1) -> bool:
        query = super().query(query, limit)

        print(query)
        self.query_builder.add(query)

        return True

    def save(self, record: ORecord, create_class=True) -> bool:
        return super().save(record, create_class=False)

    def _get_id(self, record: ORecord) -> str:
        return "$" + record._batch_id
