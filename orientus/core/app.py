from contextlib import contextmanager

from orientus.core.db import OrientUs, OrientUsDB


@contextmanager
def init_db(host: str, port: int, db_name: str, username: str, password: str) -> OrientUsDB:
    orient = OrientUs(host, port)

    odb = OrientUsDB(db_name, username, password, orient)

    yield odb

    odb.close()
