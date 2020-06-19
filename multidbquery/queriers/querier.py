"""
Querier class is used as the base class for the execution of queries in single or multiple databases.

The querier class must support parallelism with multi thread and single thread execution and return the result set
for each query in the following format:
{
    'database_name_1': [
        {
            'column_A': 'value A row 1',
            'column_B': 'value B row 1',
            '...': '...'
        },
        {
            'column_A': 'value A row 2',
            'column_B': 'value B row 2',
            '...': '...'
        },
        {
            '...': '...'
        }
    ],
    'database_name_2': [
        {
            'column_A': 'value A row 1',
            'column_B': 'value B row 1',
            '...': '...'
        },
        {
            'column_A': 'value A row 2',
            'column_B': 'value B row 2',
            '...': '...'
        },
        {
            '...': '...'
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

    @staticmethod
    def _remove_space_comments(query):
        """
        Iterates through query characters to strip comments.

        :param query: string containing query.
        :return: one-liner string containing query.
        """
        query_no_comment = list()
        find_end_comment = False

        for line in query.split('\n'):
            def check_double_hiphen(line_to_check):
                if '--' in line_to_check:
                    line_to_check = line_to_check[:line_to_check.index('--')]
                return line_to_check

            def add_line(line_to_add):
                line_to_add = check_double_hiphen(line_to_add)
                query_no_comment.append(line_to_add)

            # breaks for easier treatment of comments
            line = line.split(' ')

            if '/*' in line:
                if '*/' in line:
                    line = line[:line.index('/*')] + line[line.index('*/')+2:]
                else:
                    line = line[:line.index('/*')]
                add_line(line)
                find_end_comment = True
                continue

            if find_end_comment:
                if '*/' not in line:
                    continue
                else:
                    line = line[line.index('*/')+2:]
                    add_line(line)
                    find_end_comment = False

            add_line(line)

        query_trimmed = ' '.join(query_no_comment)
        return query_trimmed

    def _parse(self, query: str):
        """
        Parses query tokens and cause SyntaxError for tokens that match bad commands.
        bad commands are defined at the beggining of querier.py file, under bad_commands variable.

        :param query: string containing the query that will be parsed
        :return:
        """
        stripped_query = self._remove_space_comments(query)
        tokens = _sqlparse.parse(stripped_query)[0].tokens
        for tk in tokens:
            if tk.value.upper() in self.BAD:
                raise SyntaxError(f'bad command found: {tk.value.upper()}')

    @abstractmethod
    def _single_query(self, query: str, database: str) -> List[Dict[str, List]]:
        """
        Protected method for single thread query execution.

        :param query: string containing the query that will be executed.
        :param database: string containing the database name.
        :return: List of dictionaries that contain the column as key and the row value
        """
        pass

    @abstractmethod
    def _multi_query(self, query: str, database: List[str]) -> Dict[str, List[Dict]]:
        """
        Protected method for multi thread query execution.

        :param query: string containing the query that will be executed.
        :param database: string or list of strings containing the database names.
        :return: List of dictionaries that contain the column as key and the row value
        """
        pass

    @abstractmethod
    def query(self, query: str, database: Union[List[str], str], multithreading: bool = True) -> Dict[str, List[Dict]]:
        """
        Public method for query execution.

        :param query: string containing the query that will be executed.
        :param database: string or list of strings containing the database names.
        :param multithreading: toggles if multithreading is used or not.
        :return: dictionary with a key for each database executed, containing a list of dictionaries representing each
        row, with a key for each column.
        """
        pass
