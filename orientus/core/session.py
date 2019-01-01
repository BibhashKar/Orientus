from typing import ClassVar, List

from pyorient import OrientRecord, PyOrientCommandException

from orientus.core.db import OrientUsDB
from orientus.core.domain import ORecord, OVertex, OEdge


class Session:

    def __init__(self, db: OrientUsDB):
        self.db = db
        self.debug = db.debug

    def __enter__(self):
        self.connection = self.db.acquire_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.release_connection(self.connection)

    def save(self, record: ORecord) -> str:
        if self.debug:
            print('in %s' % self.save.__name__)

        clz_name = record.__class__.__name__

        from orientus.core.domain import OVertex, OEdge

        if isinstance(record, OEdge):
            return self.add_edge(record._from_vertex, record._to_vertex, record)

        if isinstance(record, OVertex):
            clz_create_cmd = 'create class %s if not exists extends V' % clz_name
        else:
            clz_create_cmd = 'create class %s if not exists' % clz_name

        if self.debug:
            print(clz_create_cmd)
            print('record class create:', self.command(clz_create_cmd))

        insert_cmd = "insert into %s set " % (clz_name) + self._fields_to_str(record)
        if self.debug: print(insert_cmd)

        result = self.command(insert_cmd)

        record._rid = result[0]._rid
        if self.debug: print('rid', result[0]._rid)

        return result[0]._rid

    def save_if_not_exists(self, record: ORecord) -> str:
        query = "select from %s where %s" % (
            record.class_name(),
            self._fields_to_str(record, delimiter='AND')
        )

        results = self.query(query)

        if len(results) > 0:
            record._rid = results[0]._rid
            record._version = results[0]._version
            return record._rid
        else:
            return self.save(record)

    def fetch(self, cls: ClassVar, rid: str) -> OrientRecord:
        query = "select from %s where @rid = '%s'" % ((cls.__name__), rid)

        results = self.query(query)

        if len(results) > 0:
            return results[0]
        else:
            return None

    def query(self, query: str, limit: int = -1) -> List[OrientRecord]:
        if limit > -1 and ' limit ' not in query:
            query = '%s limit %s' % (query, limit)

        if self.debug: print('Query:', query)

        results = self.command(query)
        if self.debug: print('Results size:', len(results))

        return results

    def update(self, record: ORecord) -> bool:
        update_cmd = "update %s set %s where @rid = '%s'" % (
            record.class_name(),
            self._fields_to_str(record),
            record._rid
        )
        if self.debug: print(update_cmd)

        results = self.command(update_cmd)
        if self.debug: print(results)

        return len(results) == 1

    def delete(self, record: ORecord) -> bool:
        if isinstance(record, OVertex):
            delete_cmd = "delete vertex %s" % record._rid
        elif isinstance(record, OEdge):
            delete_cmd = "delete edge %s" % record._rid
        else:
            delete_cmd = "delete from %s where @rid = %s" % (record.class_name(), record._rid)

        if self.debug: print(delete_cmd)

        results = self.command(delete_cmd)

        return len(results) == 1

    def add_edge(self, frm: OVertex, to: OVertex, edge: OEdge) -> str:
        assert bool(frm._rid)
        assert bool(to._rid)

        create_cmd = "create edge %s from %s to %s" % (edge.class_name(), frm._rid, to._rid)
        value_str = self._fields_to_str(edge)
        if value_str:
            create_cmd += ' set ' + value_str

        if self.debug: print(create_cmd)

        results = self.command(create_cmd)

        return results[0]._rid

    def command(self, str) -> List[OrientRecord]:
        try:
            results = self.connection.command(str)
        except PyOrientCommandException:
            return []

        return results

    def _fields_to_str(self, record, delimiter=',') -> str:
        values = []
        for field, value in record.__dict__.items():
            if field in ['_rid', '_version', '_from_vertex',
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
