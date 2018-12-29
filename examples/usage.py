from typing import Dict, ClassVar

from pyorient import OrientRecord

from core.app import init_db
from core.domain import OGraph, OVertex, OEdge, ORecord


class Animal(OVertex):
    def __init__(self, kind, name):
        self.kind = kind
        self.name = name


class File(ORecord):
    def __init__(self, filename, size):
        self.filename = filename
        self.size = size


class FriendShip(OEdge):
    pass


def sample():
    with OGraph() as db:
        v1 = Animal('Deer', 'Chapila')
        v1.save()

        v2 = Animal('Rabbit', 'Jaggu')
        v2.save()

        e1 = FriendShip()
        v1.add_edge(v2, e1).save()


def data_to_ORecord(cls: ClassVar):
    data = {'_OrientRecord__rid': '#73:0', '_OrientRecord__version': 1, '_OrientRecord__o_class': 'File',
            '_OrientRecord__o_storage': {'size': 1500, 'filename': 'demo.txt'}}
    obj: cls = type(cls.__name__, (cls,), data['_OrientRecord__o_storage'])
    # obj._rid = data._rid
    # obj._version = data._version

    return obj


def to_File(data: OrientRecord):
    values = data.__dict__['_OrientRecord__o_storage']
    f = File(values['filename'], values['size'])
    f.rid = data._rid
    f.version = data._version
    return f


if __name__ == '__main__':
    with init_db('localhost', 2424, 'knowledge', 'root', 'admin') as db:
        # v1 = Animal('Deer', 'Chapila')
        # print(v1.save())

        # rec1 = File('demo.txt', 1500)
        # print(type(rec1))
        # print(rec1.save_if_not_exists())

        # file = File.fetch('#73:0')
        f = to_File(db.fetch(File, '#73:0'))
        print(vars(f))

        # results = db.command("select from file where @rid='#73:0'")
        # result = results[0]
        # print(result.__dict__)
        # ans = data_to_ORecord(File)
        # print(ans)
        # print(file)
        # print(file.__dict__)

        # f.size = 2015
        # f.update()

        # File.fetch(rid='')

        # f.delete()
