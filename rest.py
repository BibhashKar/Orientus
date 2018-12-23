from enum import Enum
from pprint import pprint

import requests


class HttpMethod(Enum):
    HEAD = 'head'
    GET = 'get'
    POST = 'post'
    PUT = 'put'
    PATCH = 'patch'
    DELETE = 'delete'


class OProperties:
    def __init__(self, domain: str, port: int, username: str, password: str, db_name: str):
        self.domain = domain
        self.port = port
        self.username = username
        self.password = password
        self.db_name = db_name


class OConnection:

    def __init__(self, props: OProperties):
        self.props = props
        self.cookie = None

    def request(self, relative_url: str, method: HttpMethod, data=None):
        full_url = 'http://' + self.props.domain + ':' + str(self.props.port) + relative_url
        print('URL log', full_url)

        headers = {'Cookie': self.cookie}

        response = None

        authentication = ()
        if not self.cookie:
            authentication = (self.props.username, self.props.password)

        if method == HttpMethod.HEAD:
            response = requests.head(full_url, data=data, headers=headers, auth=authentication)

        if method == HttpMethod.GET:
            response = requests.get(full_url, data=data, headers=headers, auth=authentication)

        if method == HttpMethod.POST:
            response = requests.post(full_url, json=data, headers=headers)  # auth=authetication)

        if method == HttpMethod.PUT:
            response = requests.put(full_url, json=data, headers=headers)  # auth=authetication)

        if method == HttpMethod.PATCH:
            response = requests.patch(full_url, json=data, headers=headers)  # auth=authetication)

        if method == HttpMethod.DELETE:
            response = requests.delete(full_url, json=data, headers=headers)  # auth=authetication)

        return response

    def connect(self):
        db_name = self.props.db_name
        url = '/connect/{db_name}'.format(db_name=db_name)
        response = self.request(url, HttpMethod.GET)
        if response.status_code == 204:
            self.cookie = response.headers['Set-Cookie']
            return self
        else:
            raise ConnectionError('Connection Error for database {db_name}'.format(db_name=db_name))


class OConnectionPool:
    def __init__(self, props: OProperties, size: int = 10):
        self.props = props
        self.pool = []

    def initialize(self):
        conn = self.acquire()
        self.release(conn)

    def acquire(self) -> OConnection:
        if len(self.pool) == 0:
            conn = OConnection(self.props).connect()
            self.pool.append(conn)

        return self.pool.pop()

    def release(self, conn: OConnection):
        self.pool.append(conn)


class OrientDatabase:

    def __init__(self, domain: str, port: int, username: str, password: str, db_name: str):
        self.db_name = db_name
        self.props = OProperties(domain, port, username, password, db_name)

        self.__conn_pool = OConnectionPool(self.props)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def _request(self, relative_url: str, method: HttpMethod, data=None):
        # authetication = (self.props.username, self.props.password)
        response = None

        conn = self.__conn_pool.acquire()
        response = conn.request(relative_url, method, data)
        self.__conn_pool.release(conn)

        return response

    def connect(self) -> bool:
        """Connects to the specified database

        :param db_name:Database name
        :return: True if gets connected to database otherwise raises Connection Error
        """
        self.__conn_pool.initialize()
        return True

    def disconnect(self):
        # TODO: fix disconnect 401 response
        response = self._request("/disconnect", HttpMethod.GET)
        return response.status_code

    def create_database(self, db_name: str, db_type: str):
        url = "/database/{db_name}/{db_type}".format(db_name=db_name, db_type=db_type)
        response = self._request(url, HttpMethod.POST)
        return response.json()

    def delete_database(self, db_name) -> bool:
        """Returns True if database deleted else False

        :param db_name:
        :return: boolean
        """
        url = "/database/{db_name}".format(db_name=db_name)
        response = self._request(url, HttpMethod.DELETE)
        return response.status_code == 204

    def list_databases(self):
        response = self._request("/listDatabases", HttpMethod.GET)
        return response.json()

    def get_info(self):
        url = '/database/{db_name}'.format(db_name=self.db_name)
        response = self._request(url, HttpMethod.GET)
        return response.json()

    def allocation_info(self) -> dict:
        url = '/allocation/{db_name}'.format(db_name=self.db_name)
        response = self._request(url, HttpMethod.GET)
        return response.json()

    def get_class_info(self, class_name):
        url = '/class/{db_name}/{class_name}'.format(db_name=self.db_name, class_name=class_name)
        response = self._request(url, HttpMethod.GET)
        return response.json()

    def create_class(self, class_name):
        url = '/class/{db_name}/{class_name}'.format(db_name=self.db_name, class_name=class_name)
        response = self._request(url, HttpMethod.POST)
        class_id = response.json()
        return class_id

    def create_property(self, class_name, props: dict):
        """
        For more: https://orientdb.com/docs/last/OrientDB-REST.html#multiple-property-creation
        :param class_name:
        :param props:
        :return: total number of saved properties of the class
        """
        url = '/property/{db_name}/{class_name}/'.format(db_name=self.db_name, class_name=class_name)
        response = self._request(url, HttpMethod.POST, data=props)
        prop_count = response.json()
        return prop_count

    def get_cluster(self, cluster_name: str):
        url = '/cluster/{db_name}/{cluster_name}/'.format(db_name=self.db_name, cluster_name=cluster_name)
        response = self._request(url, HttpMethod.GET)
        return response.json()

    def execute(self, command: str, params: dict = {}) -> dict:
        """Returns result of sql command

        right now they are using POST for query(No GET method) and update, delete also

        For more https://orientdb.com/docs/last/OrientDB-REST.html#post---command
        :param command:SQL command to run in Database
        :param SQL parameters
        :return: :class: dict
        """
        url = '/command/{db_name}/sql'.format(db_name=self.db_name)
        response = self._request(url,
                                 HttpMethod.POST,
                                 data={'command': command, 'parameters': params})
        return response.json()

    def query(self, query_text, limit=-1, fetch_plan=1):
        url = "/query/{db_name}/sql/{query_text}/{limit}/*:{fetch_plan}"
        # if limit:
        #     url += "/{limit}"
        url = url.format(db_name=self.db_name, query_text=query_text, limit=limit, fetch_plan=fetch_plan)
        response = self._request(url, HttpMethod.GET)
        return response.json()

    def export(self):
        """Exports a gzip file that contains the database JSON export.

        :return: <database-name>.gzip file
        """
        url = '/export/{db_name}'.format(db_name=self.db_name)
        response = self._request(url, HttpMethod.GET)
        # TODO: need to solve gzip extraction
        with open(self.db_name + '.gzip', 'wb') as f:
            f.write(response.content)

    def import_db(payload):
        pass

    def get_document(self, record_id, fetch_plan=0):
        """Returns the document

        :param record_id: Record ID(https://orientdb.com/docs/last/Concepts.html#record-id)
        :param fetch_plan: Optional, is the fetch plan used. 0 means the root record,
               -1 infinite depth, positive numbers is the depth level. Look at
               Fetching Strategies for more information.
        :return: document
        """
        url = "/document/{database}/{rid}/*:{fetch_plan}" \
            .format(database=self.db_name, rid=record_id, fetch_plan=fetch_plan)
        response = self._request(url, HttpMethod.GET)
        return response.json()

    def document_exists(self, record_id) -> bool:
        """Checks if a document exists

        :param record_id: record_id: Record ID(https://orientdb.com/docs/last/Concepts.html#record-id)
        :return: :class: bool
        """
        url = "/document/{database}/{rid}".format(database=self.db_name, rid=record_id)
        response = self._request(url, HttpMethod.HEAD)
        return response.status_code == 204

    def create_document(self, doc) -> dict:
        """Creates a new document

        :param doc: document content
        :return: :class: dict
        """
        url = "/document/{database}".format(database=self.db_name)
        response = self._request(url, HttpMethod.POST, data=doc)
        return response.json()

    def update_document(self, record_id: str, payload: dict, update_mode='partial') -> dict:
        url = "/document/{database}/{rid}?updateMode={update_mode}" \
            .format(database=self.db_name, rid=record_id, update_mode=update_mode)
        response = self._request(url, HttpMethod.PUT, data=payload)
        return response.json()

    def patch_document(self, record_id: str, payload: dict) -> dict:
        pass

    def delete_document(self, record_id: str) -> None:
        url = "/document/{database}/{rid}".format(database=self.db_name, rid=record_id)
        self._request(url, HttpMethod.DELETE)

    def get_document_by_class(self, class_name: str, record_position: str, fetch_plan: int = 0) -> dict:
        url = "/documentbyclass/{database}/{class_name}/{record_position}/*:{fetch_plan}" \
            .format(database=self.db_name,
                    class_name=class_name,
                    record_position=record_position,
                    fetch_plan=fetch_plan)
        response = self._request(url, HttpMethod.GET)
        return response.json()

    def document_exists_by_class(self, class_name: str, record_position: str) -> bool:
        url = "/documentbyclass/{database}/{class_name}/{record_position}" \
            .format(database=self.db_name,
                    class_name=class_name,
                    record_position=record_position)
        response = self._request(url, HttpMethod.HEAD)
        return response.status_code == 204

    def get_record(self, index_name: str, key: str) -> dict:
        pass

    def get_server_info(self):
        url = "/server"
        response = self._request(url, HttpMethod.GET)
        return response.json()

    def set_server_config(self, name, value) -> bool:
        url = "/server/{setting_name}/{setting_value}".format(setting_name=name, setting_value=value)
        response = self._request(url, HttpMethod.POST)
        return response.status_code == 200


if __name__ == '__main__':
    with OrientDatabase('localhost', 2480, 'root', 'admin', 'demodb') as db:
        db.connect()
        pprint(db.get_info())
    # props = OProperties('localhost', 2480, 'root', 'admin', 'demodb')
    # conn = OConnection(props)
    # ans, session_id_content = conn.connect()
    # session_id = session_id_content.split('=')[1]
    # print(session_id)
    # session_id_content = 'OSESSIONID=OS15454155705565390898342536433870'
    # response = requests.get("http://localhost:2480/database/demodb",
    #                         headers={'OSESSIONID': session_id})
    # headers={'Cookie': session_id_content})
    # pprint(response.headers)
    # print(response.status_code)
    # pprint(response.json())
