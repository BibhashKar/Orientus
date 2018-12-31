from orientus.core.db import OrientUsDB
from orientus.core.domain import ORecord


class Session:

    def get_db(self):
        return OrientUsDB.get_db()

    def fetch(self, rid: str):
        return self.get_db().fetch(self, rid)

    def query(self, query: str):
        return self.get_db().query(query)

    def save(self, record: ORecord) -> str:
        return self.get_db().save(record)

    def save_if_not_exists(self, record: ORecord):
        return self.get_db().save_if_not_exists(record)

    def update(self, record: ORecord):
        return self.get_db().update(record)

    def delete(self, record: ORecord):
        return self.get_db().delete(record)
