"""
ODBC implementation of Querier class.
"""
import pyodbc as _pyodbc
import multiprocessing.dummy as multithread
import multiprocessing as multiproc
import functools

from typing import Dict, List, Union
from multidbquery.queriers.querier import QuerierBasic


class QuerierODBC(QuerierBasic):

    def __init__(self, driver: str, server: str, user: str, password: str, port: int = 1433):
        self._driver = driver
        self._server = server
        self._port = port
        self._username = user
        self._password = password

    def _get_cursor(self, database: str) -> _pyodbc.Cursor:
        """
        Returns pyodbc cursor for specified database.

        :param database: String containing database name
        :return: pyodbc.Cursor instance
        """
        if self.TEST_MODE:
            return None
        connstring = f'DRIVER={self._driver};SERVER={self._server};DATABASE={database};' \
                     f'PORT={self._port};UID={self._username};PWD={self._password}'

        conn = _pyodbc.connect(connstring)

        return conn.cursor()

    def _single_query(self, query: str, database: str) -> List[Dict[str, List]]:
        cursor = self._get_cursor(database)
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        result_set = []
        for row in cursor.fetchall():
            result_set.append(dict(zip(columns, row)))

        cursor.close()

        return result_set

    def _multi_query(self, query: str, database: List[str]) -> Dict[str, List[Dict]]:
        pool = multithread.Pool(multiproc.cpu_count())
        query_func = functools.partial(self._single_query, query)
        results = pool.map(query_func, database)
        pool.close()
        pool.join()

        results = [result for result in results]
        results = dict(zip(database, results))

        return results

    def query(self, query: str, database: Union[List[str], str], multithreading: bool = True) -> Dict[str, List[Dict]]:
        self._parse(query)
        result_set = {}
        if multithreading and isinstance(database, List):
            result_set = self._multi_query(query, database)
        else:
            if isinstance(database, List):
                for db in database:
                    tmp_result = self._single_query(query, db)
                    result_set[db] = tmp_result
            else:
                tmp_result = self._single_query(query, database)
                result_set[database] = tmp_result

        return result_set
