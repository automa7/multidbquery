import unittest
import multidbquery as mdq
from mock import patch


class TestQuerierODBC(unittest.TestCase):
    BAD_QUERIES = [
        'SELECT * FROM TABLE GO UPDATE TABLE SET A = 1 GO',
        'SELECT * FROM TABLE GO TRUNCATE TABLE GO',
        'SELECT * FROM TABLE GO DELETE TABLE GO',
        'DECLARE @A DATETIME SELECT * FROM TABLE WHERE COLUMN = @A GO',
        'DECLARE @A DATETIME SET @A = 1 SELECT * FROM TABLE WHERE COLUMN = @A GO'
    ]
    TEST_QUERY = "SELECT * FROM TABLE"
    TEST_DATABASES = ['DB1', 'DB2', 'DB3']

    def _assert_result_set_is_dict_of_lists(self, result_dict):
        self.assertEqual(len(result_dict), len(self.TEST_DATABASES))
        self.assertIsInstance(result_dict, dict)
        for result_set in result_dict.values():
            self.assertIsInstance(result_set, list)
            for item in result_set:
                self.assertIsInstance(item, dict)

    def setUp(self):
        self.mdq = mdq.QuerierODBC(driver='{test}', server='test', user='user', password='password')
        self.mdq.TEST_MODE = True

    @patch('multidbquery.queriers.querierodbc._pyodbc')
    def test_get_cursor(self, pyodbc_mock):
        self.mdq.TEST_MODE = False
        database = 'DB'
        connstring = f'Driver={self.mdq._driver};Server={self.mdq._server};Database={database};' \
                     f'UID={self.mdq._username};PWD={self.mdq._password}'
        self.mdq._get_cursor(database)
        pyodbc_mock.connect.assert_called_with(connstring)
        self.mdq.TEST_MODE = True

    def test_bad_query_parsing_is_blocking(self):
        for query in self.BAD_QUERIES:
            with self.assertRaises(SyntaxError):
                self.mdq._parse(query)

    def test_bad_queries_are_blocked(self):
        for query in self.BAD_QUERIES:
            with self.assertRaises(SyntaxError):
                self.mdq.query(query, 'localhost', multithreading=False)

    @patch('multidbquery.QuerierODBC._single_query')
    def test_single_query_is_called_and_returns_dict_of_lists(self, _single_query_mock):

        _single_query_mock.return_value = [{'A': 1, 'B': 2}, {'A': 2, 'B': 3}]
        results = self.mdq.query(self.TEST_QUERY, self.TEST_DATABASES, multithreading=False)

        _single_query_mock.assert_called_with(self.TEST_QUERY, 'DB3')
        self._assert_result_set_is_dict_of_lists(results)

    @patch('multidbquery.QuerierODBC._multi_query')
    def test_querying_multi_is_called(self, mocker):
        self.mdq.query(self.TEST_QUERY, self.TEST_DATABASES, multithreading=True)
        mocker.assert_called_with(self.TEST_QUERY, self.TEST_DATABASES)

    @patch('multidbquery.QuerierODBC._single_query')
    def test_multi_query_retuns_dict_of_lists(self, _single_query_mock):
        _single_query_mock.return_value = [{'A': 1, 'B': 2}, {'A': 2, 'B': 3}]
        results = self.mdq._multi_query(self.TEST_QUERY, self.TEST_DATABASES)
        self._assert_result_set_is_dict_of_lists(results)




if __name__ == '__main__':
    unittest.main()
