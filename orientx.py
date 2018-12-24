import socket
import struct

from pyorient import OrientSocket, SOCK_CONN_TIMEOUT, FIELD_SHORT, PyOrientConnectionPoolException, SUPPORTED_PROTOCOL, \
    PyOrientWrongProtocolVersionException, PyOrientConnectionException, OrientDB, OrientSerialization
from pyorient.utils import dlog


class OrientUsSocket(OrientSocket):
    """Code taken from pyorient.OrientSocket"""

    def connect(self):
        '''Connects to the inner socket
                could raise :class:`PyOrientConnectionPoolException`
                '''
        dlog("Trying to connect...")
        try:
            self._socket.settimeout(SOCK_CONN_TIMEOUT)  # 30 secs of timeout
            self._socket.connect((self.host, self.port))
            _value = self._socket.recv(FIELD_SHORT['bytes'])

            if len(_value) != 2:
                self._socket.close()

                raise PyOrientConnectionPoolException(
                    "Server sent empty string", []
                )

            self.protocol = struct.unpack('!h', _value)[0]
            if self.protocol > SUPPORTED_PROTOCOL + 1:  # adding 1 to support Orient DB 3.0 and later
                raise PyOrientWrongProtocolVersionException(
                    "Protocol version " + str(self.protocol) +
                    " is not supported yet by this client.", [])
            self.connected = True
        except socket.error as e:
            self.connected = False
            raise PyOrientConnectionException("Socket Error: %s" % e, [])


class OrientUsDB(OrientDB):

    def __init__(self, host='localhost', port=2424, serialization_type=OrientSerialization.CSV):
        super().__init__(host, port, serialization_type)

        if not isinstance(host, OrientSocket):
            connection = OrientUsSocket(host, port, serialization_type)
        else:
            connection = host

        self._connection = connection


if __name__ == '__main__':
    client = OrientUsDB('localhost', 2424)
    client.connect('root', 'admin')
    print(client.db_open('knowledge', 'root', 'admin'))
