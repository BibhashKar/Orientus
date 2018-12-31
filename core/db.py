from typing import ClassVar, List

import pyorient
from pyorient import OrientSocket, PyOrientWrongProtocolVersionException, OrientDB, OrientSerialization, OrientRecord


class OrientUsSocket(OrientSocket):
    """Code taken from pyorient.OrientSocket"""

    def connect(self):
        '''Connects to the inner socket
                could raise :class:`PyOrientConnectionPoolException`'''
        try:
            super().connect()
        except PyOrientWrongProtocolVersionException:
            self.connected = True


class OrientUs(OrientDB):

    def __init__(self, host='localhost', port=2424, serialization_type=OrientSerialization.CSV):
        super().__init__(host, port, serialization_type)

        if not isinstance(host, OrientUsSocket):
            connection = OrientUsSocket(host, port, serialization_type)
        else:
            connection = host

        self._connection = connection


class OrientUsDB:
    from core.domain import ORecord
    from core.domain import OVertex, OEdge

    import threading
    thread_local = threading.local()

    @classmethod
    def get_db(cls) -> 'OrientUsDB':
        return cls.thread_local.db

    def __init__(self, db_name: str, username: str, password: str, orient: OrientUs):
        self.db_name = db_name
        self.orient = orient

        self.orient.db_open(db_name, username, password)

        OrientUsDB.thread_local.db: OrientUsDB = self

        print("-- Initializing '%s' orientus database --" % (self.db_name))
        print()

    def save(self, record: ORecord) -> str:
        print('in %s' % OrientUsDB.save.__name__)

        clz_name = record.__class__.__name__

        from core.domain import OVertex, OEdge

        if isinstance(record, OEdge):
            return self.add_edge(record._from_vertex, record._to_vertex, record)

        if isinstance(record, OVertex):
            clz_create_cmd = 'create class %s if not exists extends V' % clz_name
        else:
            clz_create_cmd = 'create class %s if not exists' % clz_name
        print(clz_create_cmd)

        print('record class create:', self.command(clz_create_cmd))

        insert_cmd = "insert into %s set " % (clz_name) + self._fields_to_str(record)
        print(insert_cmd)
        result = self.command(insert_cmd)

        record._rid = result[0]._rid
        print('rid', result[0]._rid)

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
        if limit > -1 and not ' limit ' in query:
            query = '%s limit %s' % (query, limit)

        print('Query:', query)

        results = self.command(query)
        print('Results size:', len(results))

        return results

    def update(self, record: ORecord) -> bool:
        update_cmd = "update %s set %s where @rid = '%s'" % (
            record.class_name(),
            self._fields_to_str(record),
            record._rid
        )
        print(update_cmd)

        results = self.command(update_cmd)
        print(results)

        return len(results) == 1

    def delete(self, record: ORecord) -> bool:
        if isinstance(record, OVertex):
            delete_cmd = "delete vertex %s" % record._rid
        elif isinstance(record, OEdge):
            delete_cmd = "delete edge %s" % record._rid
        else:
            delete_cmd = "delete from %s where @rid = %s" % (record.class_name(), record._rid)

        print(delete_cmd)

        results = self.command(delete_cmd)

        return len(results) == 1

    def add_edge(self, frm: OVertex, to: OVertex, edge: OEdge) -> str:
        assert bool(frm._rid)
        assert bool(to._rid)

        create_cmd = "create edge %s from %s to %s" % (edge.class_name(), frm._rid, to._rid)
        value_str = self._fields_to_str(edge)
        if value_str:
            create_cmd += ' set ' + value_str

        print(create_cmd)

        results = self.command(create_cmd)

        return results[0]._rid

    def command(self, str) -> List[OrientRecord]:
        try:
            results = self.orient.command(str)
        except pyorient.PyOrientCommandException as pyoe:
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

    def close(self):
        print()
        print("-- Closing '%s' orientus database --" % self.db_name)

        self.orient.close()
