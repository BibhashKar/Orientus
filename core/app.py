from contextlib import contextmanager

from core.db import OrientUs, OrientUsDB


@contextmanager
def init_db(host: str, port: int, db_name: str, username: str, password: str) -> OrientUsDB:
    orient = OrientUs(host, port)

    db = OrientUsDB(db_name, username, password, orient)

    yield db

    db.close()
