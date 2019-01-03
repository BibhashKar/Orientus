from examples.db_init import Doc, File
from orientus.core.db import OrientUsDB
from orientus.core.session import BatchSession

if __name__ == '__main__':
    with OrientUsDB('test', 'root', 'admin', debug=True) as db:

        with BatchSession(db) as session:

            # doc = Doc("I am Kelvin Clan. I am the brand. I am fashion.")
            file = File('demo.txt', 1500)
            session.upsert(file)