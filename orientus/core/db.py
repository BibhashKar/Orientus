import time
from collections import deque
from threading import Thread
from typing import Deque

import pyorient
from pyorient import OrientSocket, PyOrientWrongProtocolVersionException, OrientDB, OrientSerialization


class OrientUsSocket(OrientSocket):

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

    def recreate_db(self, db_name: str, db_type=pyorient.DB_TYPE_GRAPH, storage_type=pyorient.STORAGE_TYPE_PLOCAL):
        if self.db_exists(db_name):
            self.db_drop(db_name)
            self.db_create(db_name, db_type, storage_type)
        else:
            self.db_create(db_name, db_type, storage_type)


class OrientUsDB(Thread):

    def __init__(self, db_name: str, username: str, password: str,
                 host: str = 'localhost', port: int = 2424, debug=False):
        super().__init__()

        self.host = host
        self.port = port
        self.db_name = db_name
        self.username = username
        self.password = password
        self.debug = debug

        self.connection_pool: Deque[OrientUs] = deque(maxlen=10)

        self.create_connection()

        self.keep_running = True

        OrientUsDB.db = self

        if self.debug:
            print("-- Initializing '%s' orientus database --" % (self.db_name))
            print()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def stop(self):
        if self.debug:
            print()
            print("-- Closing '%s' orientus database --" % self.db_name)

        for conn in self.connection_pool:
            conn.close()

        self.stop_task()

    def run(self):
        while self.keep_running:
            time.sleep(10)

    def create_connection(self):
        try:
            connection = OrientUs(self.host, self.port)
            connection.db_open(self.db_name, self.username, self.password)

            self.connection_pool.append(connection)
        except Exception as e:
            print('Error occured during creating connection:', e)
            self.stop_task()

    def acquire_connection(self):
        try:
            connection = self.connection_pool.popleft()
        except IndexError:
            # TODO: give more meaningful error 'IndexError: pop from an empty deque' when database isn't started
            self.create_connection()
            connection = self.connection_pool.popleft()

        return connection

    def release_connection(self, connection):
        self.connection_pool.append(connection)

    def connection_count(self):
        return len(self.connection_pool)

    def stop_task(self):
        self.keep_running = False
