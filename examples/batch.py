from examples.db_init import TestDoc, File
from orientus.core.db import OrientUsDB
from orientus.core.session import BatchSession

if __name__ == '__main__':
    import MySQLdb

    print(MySQLdb.escape_string("''"))
    print(MySQLdb.escape_string("don't"))

    # with OrientUsDB('test', 'root', 'admin', debug=True) as db:
    #
    #     with BatchSession(db) as session:

            # doc = Doc("I am Kelvin Clan. I am the brand. I am fashion.")
            # file = File('demo.txt', 1500)
            # session.update(file)