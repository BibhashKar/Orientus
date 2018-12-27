from contextlib import contextmanager

from core.db import OrientUs, OrientUsDB
from core.domain import OVertex


@contextmanager
def init_db(host: str, port: int, db_name: str, username: str, password: str):
    orient = OrientUs(host, port)

    db = OrientUsDB(db_name, username, password, orient)

    yield db

    db.close()


if __name__ == '__main__':
    class Animal(OVertex):
        def __init__(self, kind, name):
            self.kind = kind
            self.name = name


    with init_db('localhost', 2424, 'knowledge', 'root', 'admin') as db:
        v1 = Animal('Deer', 'Chapila')
        v1.save()
