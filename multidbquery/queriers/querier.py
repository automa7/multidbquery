"""
Querier class is used as the base class for the execution of queries in single or multiple databases.

The querier class must support parallelism with multi thread and single thread execution and return the result set
for each query in the following format:
{
    'database_name_1': [
        {
            'column__A': ['value1', 'value2', '...'],
            'column_B': ['value1', 'value2', '...'],
            '...': ['...']
        }
    ],
    'database_name_2': [
        {
            'column__A': ['value1', 'value2', '...'],
            'column_B': ['value1', 'value2', '...'],
            '...': ['...']
        }
    ]
}
"""
import sqlparse as _sqlparse
from typing import Dict, List, Union
from abc import ABC, abstractmethod

bad_commands = ["UPDATE", "DELETE", "TRUNCATE", "DECLARE", "SET"]


class QuerierBasic(ABC):
    BAD = bad_commands
    TEST_MODE = False

    def _parse(self, query: str):
        """
        Parses query tokens and cause SyntaxError for tokens that match bad commands.
        bad commands are defined at the beggining of querier.py file, under bad_commands variable.

        :param query: string containing the query that will be parsed
        :return:
        """
        query = _sqlparse.parse(query)[0].tokens
        for tk in query:
            if tk.value.upper() in self.BAD:
                raise SyntaxError(f'bad command found: {tk.value.upper()}')

    @abstractmethod
    def _single_query(self, query: str, database: str) -> List[Dict[str, List]]:
        """
        protected method for single thread query execution.

        :param query: string containing the query that will be executed
        :param database: string containing the database name.
        :return: List of dictionaries that contain the column as key and the row value
        """
        pass

    @abstractmethod
    def _multi_query(self, query: str, database: List[str]) -> Dict[str, List[Dict]]:
        """
        protected method for multi thread query execution.

        :param query: string containing the query that will be executed.
        :param database: string or list of strings containing the database names.
        :return: List of dictionaries that contain the column as key and the row value
        """
        pass

    @abstractmethod
    def query(self, query: str, database: Union[List[str], str], multithreading: bool = True) -> Dict[str, List[Dict]]:
        """
        public method for query execution.

        :param query: string containing the query that will be executed.
        :param database: string or list of strings containing the database names.
        :param multithreading: toggles if multithreading is used or not.
        :return: dictionary with a key for each database executed, containing a list of dictionaries representing each
        row, with a key for each column.
        """
        pass
