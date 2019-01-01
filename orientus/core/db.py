import time
from collections import deque
from threading import Thread
from typing import Deque

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


class OrientUsDB(Thread):

    def __init__(self, db_name: str, username: str, password: str,
                 host: str = 'localhost', port: int = 2424, ):
        super().__init__()

        self.host = host
        self.port = port
        self.db_name = db_name
        self.username = username
        self.password = password

        self.connection_pool: Deque[OrientUs] = deque(maxlen=10)

        self.create_connection()

        self.keep_running = True

        OrientUsDB.db = self

        print("-- Initializing '%s' orientus database --" % (self.db_name))
        print()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def stop(self):
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
            self.create_connection()
            connection = self.connection_pool.popleft()

        return connection

    def release_connection(self, connection):
        self.connection_pool.append(connection)

    def connection_count(self):
        return len(self.connection_pool)

    def stop_task(self):
        self.keep_running = False
