import pyodbc as _pyodbc
from typing import Dict, List, Union
from .querier import QuerierBasic


class QuerierODBC(QuerierBasic):

    def __init__(self, driver: str, server: str, user: str, password: str, port: int = 1433):
        self._driver = driver
        self._server = server + ':' + str(port)
        self._username = user
        self._password = password

    def _connect(self, database: str):
        conn = _pyodbc.connect(f'Driver={self._driver};'
                               f'Server={self._server};'
                               f'Database={database};'
                               f'UID={self._username};'
                               f'PWD={self._password}')
        return conn

    def _single_query(self, query: str, database: str) -> List[str]:
        conn = self._connect(database)
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        conn.close()

        return rows

    def _multi_query(self, query: str, database: List[str]) -> List[str]:
        pass

    def query(self, query: str, database: Union[List[str], str], multithreading: bool = True) -> Dict[str, List[str]]:
        self._parse(query)
        result_set = {}
        if multithreading and isinstance(database, list()):
            result_set = self._multi_query(query, database)
        else:
            if not isinstance(database, str):
                for db in database:
                    tmp_result = self._single_query(query, db)
                    result_set[db] = tmp_result
            else:
                tmp_result = self._single_query(query, database)
                result_set[database] = tmp_result

        return result_set
