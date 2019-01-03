from abc import ABC
from collections import OrderedDict
from typing import ClassVar, List, Optional

from pyorient import OrientRecord, PyOrientCommandException

from orientus.core.db import OrientUsDB
from orientus.core.domain import ORecord, OVertex, OEdge


class BatchHolder:

    def __init__(self):
        self.index = 0
        self.obj_dict = {}
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

    def finalize(self) -> str:
        result = ["let %s = %s" % (variable, query) for variable, query in self.query_dict.items()]
        return "\n".join(["begin;", ";".join(result), "commit retry 10;"])


class AbstractSession(ABC):

    def __init__(self, db: OrientUsDB):
        self.db = db
        self.debug = db.debug

    def __enter__(self):
        self.connection = self.db.acquire_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.release_connection(self.connection)

    def command(self, str) -> List[OrientRecord]:
        try:
            results = self.connection.command(str)
        except PyOrientCommandException:
            return []

        return results

    def query(self, query: str, limit: int = -1) -> str:
        if limit > -1 and ' limit ' not in query:
            query = '%s limit %s' % (query, limit)

        if self.debug: print('Query:', query)

        return query

    def fetch(self, cls: ClassVar, rid: str) -> str:
        query = "select from %s where @rid = '%s'" % ((cls.__name__), rid)
        return query

    def _process_dml(self, statement: str, record: ORecord = None) -> str:
        if self.debug: print(statement)
        return statement

    def _add_edge(self, frm: OVertex, to: OVertex, edge: OEdge) -> bool:
        frm_id = self._record_id(frm)
        to_id = self._record_id(to)

        assert bool(frm_id)
        assert bool(to_id)

        create_cmd = "create edge %s from %s to %s" % (edge.class_name(), frm_id, to_id)
        value_str = self._fields_to_str(edge)
        if value_str:
            create_cmd += ' set ' + value_str

        self._process_dml(create_cmd, edge)

        return True

    def save(self, record: ORecord) -> bool:
        clz_name = record.__class__.__name__

        if isinstance(record, OEdge):
            return self._add_edge(record._from_vertex, record._to_vertex, record)

        if isinstance(record, OVertex):
            clz_create_cmd = 'create class %s if not exists extends V' % clz_name
        else:
            clz_create_cmd = 'create class %s if not exists' % clz_name

        self._process_dml(clz_create_cmd)

        insert_cmd = "insert into %s set " % (clz_name) + self._fields_to_str(record)

        self._process_dml(insert_cmd, record)

        return True

    # TODO
    def save_if_not_exists(self, record: ORecord) -> bool:
        query = "select from %s where %s" % (
            record.class_name(),
            self._fields_to_str(record, delimiter='AND')
        )

        results = self.query(query)

        if len(results) > 0:
            record._rid = results[0]._rid
            record._version = results[0]._version
            return True
        else:
            return self.save(record)

    def upsert(self, record: ORecord) -> bool:
        update_cmd = "update %s set %s upsert where %s" % (
            record.class_name(),
            self._fields_to_str(record),
            self._fields_to_str(record, delimiter='AND')
        )

        self._process_dml(update_cmd, record)

        return True

    def update(self, record: ORecord) -> bool:
        update_cmd = "update %s set %s where @rid = '%s'" % (
            record.class_name(),
            self._fields_to_str(record),
            self._record_id(record)
        )

        self._process_dml(update_cmd, record)

        return True

    def delete(self, record: ORecord) -> bool:
        id = self._record_id(record)

        if isinstance(record, OVertex):
            delete_cmd = "delete vertex %s" % id
        elif isinstance(record, OEdge):
            delete_cmd = "delete edge %s" % id
        else:
            delete_cmd = "delete from %s where @rid = %s" % (record.class_name(), id)

        self._process_dml(delete_cmd, record)

        return True

    def _record_id(self, record: ORecord) -> str:
        pass

    def _fields_to_str(self, record, delimiter=',') -> str:
        values = []
        for field, value in record.__dict__.items():
            if field in ['_batch_id', '_rid', '_version', '_from_vertex',
                         '_to_vertex']:  # TODO: business domain object can have these field name?
                continue
            modified_val = "'%s'" % value if type(value) == str else value
            values.append("%s = %s" % (field, modified_val))
        return (' %s ' % delimiter).join(values)

    def _data_to_ORecord(self, data: OrientRecord, cls) -> ORecord:
        obj = type(cls.__name__, (cls,), data.__dict__['_OrientRecord__o_storage'])
        obj._rid = data._rid
        obj._version = data._version

        return obj


class Session(AbstractSession):

    def query(self, query: str, limit: int = -1) -> List[OrientRecord]:
        query = super().query(query, limit)

        results = self.command(query)
        if self.debug: print('Results size:', len(results))

        return results

    def fetch(self, cls: ClassVar, rid: str) -> Optional[OrientRecord]:
        results = self.query(super().fetch(cls, rid))

        if len(results) > 0:
            return results[0]
        else:
            return None

    def _process_dml(self, statement: str, record: ORecord = None) -> bool:

        super()._process_dml(statement, record)

        if self.debug: print(statement)

        results = self.command(statement)

        if len(results) > 1:
            record._rid = results[0]._rid
            record._version = results[0]._version

            if self.debug: print('rid', results[0]._rid)

        return True

    def _record_id(self, record: ORecord) -> str:
        return record._rid


class BatchSession(AbstractSession):

    def __init__(self, db: OrientUsDB):
        super().__init__(db)

        self.batch_holder = BatchHolder()

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)

        if self.debug: print(self.batch_holder.finalize())

        result = self.connection.batch(self.batch_holder.finalize() + "return $File1;")
        # for r in result:
        #     print(r._rid)

    def query(self, query: str, limit: int = -1) -> bool:
        query = super().query(query, limit)

        print(query)
        self.batch_holder.add(query)

        return True

    def _process_dml(self, statement: str, record: ORecord = None) -> bool:
        super()._process_dml(statement)
        self.batch_holder.add(statement, record)
        return True

    def _record_id(self, record: ORecord) -> str:
        return record._batch_id
