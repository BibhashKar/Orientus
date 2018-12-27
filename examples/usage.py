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


if __name__ == '__main__':
    with init_db('localhost', 2424, 'knowledge', 'root', 'admin') as db:
        # v1 = Animal('Deer', 'Chapila')
        # print(v1.save())

        rec1 = File('demo.txt', 1500)
        rec1.save()

        File.fetch(rid='')
