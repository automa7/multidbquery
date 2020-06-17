import sqlparse as _sqlparse
from abc import ABC, abstractmethod

bad_commands = ["UPDATE", "DELETE", "TRUNCATE", "DECLARE", "SET"]


class QuerierBasic(ABC):
    BAD = bad_commands

    @abstractmethod
    def _connect(self, database):
        pass

    def _parse(self, query: str):
        query = _sqlparse.parse(query)[0].tokens
        for tk in query.tokens:
            if tk.value.upper() in self.BAD:
                raise SyntaxError('bad word found: ', tk.value.upper())

    @abstractmethod
    def _single_query(self):
        pass

    @abstractmethod
    def _multi_query(self):
        pass

    @abstractmethod
    def query(self):
        pass
