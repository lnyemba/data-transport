"""
This file will be intended to handle duckdb database
"""

import duckdb
from transport.common import Reader,Writer

class Duck(Reader):
    def __init__(self,**_args):
        super().__init__(**_args)
        self._path = None if 'path' not in _args else _args['path']
        self._handler = duckdb.connect() if not self._path else duckdb.connect(self._path)


class DuckReader(Duck) :
    def __init__(self,**_args):
        super().__init__(**_args)
    def read(self,**_args) :
        pass