import threading


class OrientUsGlobals:
    db_thread_local = threading.local()


def get_db():
    return OrientUsGlobals.db_thread_local.db
