from pyorient import OrientRecord

from orientus.core.db import OrientUsDB
from orientus.core.domain import OVertex, OEdge, ORecord
from orientus.core.session import Session


class Animal(OVertex):
    def __init__(self, kind, name):
        super().__init__()
        self.kind = kind
        self.name = name


class File(ORecord):
    def __init__(self, filename, size):
        super().__init__()

        self.filename = filename
        self.size = size


class Person(OVertex):
    def __init__(self, name):
        super().__init__()
        self.name = name


class Related(OEdge):
    def __init__(self):
        super().__init__()
        self.relation_type = 'love'


class FriendShip(OEdge):
    pass


def to_File(data: OrientRecord):
    values = data.__dict__['_OrientRecord__o_storage']
    f = File(values['filename'], values['size'])
    f._rid = data._rid
    f._version = data._version
    return f


if __name__ == '__main__':
    with OrientUsDB('test', 'root', 'admin') as db:
        with Session(db) as session:
            session.save_if_not_exists(rec1)
