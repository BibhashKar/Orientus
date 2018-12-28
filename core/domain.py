from enum import Enum
from typing import List, Optional


# from . import OrientUsDB

class ORID:
    PREFIX = '#'
    SEPARATOR = ':'

    def __init__(self, rid):
        self.rid = rid

    def get_cluster(self) -> int:
        pass

    def get_cluster_position(self) -> int:
        pass


import core.db


class ORecord:

    @classmethod
    def fetch(cls, rid: str):
        return core.db.OrientUsDB.get_db().fetch(rid)

    @classmethod
    def query(cls, query: str):
        return core.db.OrientUsDB.get_db().query(query)

    def class_name(self) -> str:
        return self.__class__.__name__

    def get_identity(self) -> ORID:
        pass

    def save(self):
        return core.db.OrientUsDB.get_db().save(self)

    def save_if_not_exists(self):
        return core.db.OrientUsDB.get_db().save_if_not_exists(self)

    def update(self):
        return core.db.OrientUsDB.get_db().update(self)

    def delete(self):
        return core.db.OrientUsDB.get_db().delete(self)


class OElement(ORecord):

    def get_property_names(self) -> List[str]:
        pass

    def is_vertex(self) -> bool:
        pass

    def is_edge(self) -> bool:
        pass

    def as_vertex(self) -> Optional['OVertex']:
        pass

    def as_edge(self) -> Optional['OEdge']:
        pass


class OBlob(ORecord):
    def from_input(self, input):
        pass

    def to_output(self, output):
        pass


class ODirection(Enum):
    IN = 'IN'
    OUT = 'OUT'
    BOTH = 'BOTH'


class OVertex(OElement):
    def add_edge(self, to: 'OVertex', edge_class: 'OEdge'):
        pass

    def get_edges(self, direction: ODirection) -> List['OEdge']:
        pass

    def get_vertices(self, direction: ODirection) -> List['OVertex']:
        pass


class OEdge(OElement):
    def get_from(self) -> OVertex:
        pass

    def get_to(self) -> OVertex:
        pass


class OGraph:
    pass
