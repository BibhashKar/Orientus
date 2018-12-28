from typing import ClassVar, List, Optional, Dict

from pyorient import OrientSocket, PyOrientWrongProtocolVersionException, OrientDB, OrientSerialization, OrientRecord

from core.domain import OVertex


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
    from core.domain import ORecord, OVertex, OEdge

    import threading
    thread_local = threading.local()

    @classmethod
    def get_db(cls) -> 'OrientUsDB':
        return cls.thread_local.db

    def __init__(self, db_name: str, username: str, password: str, orient: OrientUs):
        self.db_name = db_name
        self.orient = orient

        self.orient.db_open(db_name, username, password)

        # ctx.OrientUsGlobals.db_thread_local.db: OrientUsDB = self
        OrientUsDB.thread_local.db: OrientUsDB = self

    def save(self, record: ORecord) -> str:
        print('in %s' % OrientUsDB.save.__name__)

        is_vertex = isinstance(record, OVertex)

        clz_name = record.__class__.__name__

        if is_vertex:
            clz_create_cmd = 'create class %s if not exists extends V' % clz_name
        else:
            clz_create_cmd = 'create class %s if not exists' % clz_name
        print(clz_create_cmd)

        print('record class create:', self.orient.command(clz_create_cmd))

        insert_cmd = "insert into %s set " % (clz_name) + self._fields_to_str(record)
        print(insert_cmd)
        result: List[OrientRecord] = self.orient.command(insert_cmd)

        print('rid', result[0]._rid)
        return result[0]._rid

    def save_if_not_exists(self, record: ORecord) -> Optional[str]:
        query = "select from %s where %s" % (
            record.class_name(),
            self._fields_to_str(record, delimiter='AND')
        )

        results = self.query(query)

        if len(results) > 0:
            return None
        else:
            return self.save(record)

    def fetch(self, cls: ClassVar, rid: str) -> Optional[OrientRecord]:
        query = "select from %s where @rid = '%s'" % ((cls.__name__), rid)

        results = self.query(query)

        if len(results) > 0:
            return results[0]
        else:
            return None

    def query(self, query: str, limit: int = -1) -> List[ORecord]:
        if limit > -1 and not ' limit ' in query:
            query = '%s limit %s' % (query, limit)

        print('Query:', query)

        results = self.orient.command(query)
        print('Results size:', len(results))

        return results

    def update(self, record: ORecord) -> Optional[OrientRecord]:
        update_cmd = "update %s set %s where @rid = '%s'" % (
            record.class_name(),
            self._fields_to_str(record),
            record._rid
        )
        print(update_cmd)

        result: List[OrientRecord] = self.orient.command(update_cmd)

        return result[0] if result is not None else None

    def delete(self, record: ORecord) -> ORecord:
        pass

    def add_edge(self, frm: OVertex, to: OVertex, edge: OEdge) -> OEdge:
        pass

    def _fields_to_str(self, record, delimiter=',') -> str:
        values = []
        for field, value in record.__dict__.items():
            modified_val = "'%s'" % value if type(value) == str else value
            values.append("%s = %s" % (field, modified_val))
        return (' %s ' % delimiter).join(values)

    def close(self):
        print()
        print('-- Closing %s database --' % self.db_name)

        self.orient.close()
