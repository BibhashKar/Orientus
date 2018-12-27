from pyorient import OrientSocket, PyOrientWrongProtocolVersionException, OrientDB, OrientSerialization

from core.context import OrientUsGlobals
from core.domain import ORecord


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

    def __init__(self, db_name: str, username: str, password: str, orient: OrientUs):
        self.db_name = db_name
        self.orient = orient

        self.orient.db_open(db_name, username, password)

        OrientUsGlobals.db_thread_local.db = self

    def save(self, record: ORecord):
        print('in %s' % OrientUsDB.save.__name__)

        clz_name = record.__class__.__name__
        # print(clz_name)

        clz_create_cmd = 'create class %s if not exists extends V' % clz_name
        print(clz_create_cmd)
        print(self.orient.command(clz_create_cmd))

        values = []
        for field, value in record.__dict__.items():
            modified_val = "'%s'" % value if type(value) == str else value
            values.append("%s = %s" % (field, modified_val))

        insert_cmd = "insert into %s set " % (clz_name) + ', '.join(values)
        print(insert_cmd)
        self.orient.command(insert_cmd)

    def close(self):
        print('Closing %s' % self.db_name)
        self.orient.close()
