"""
This module implements the handler for duckdb (in memory or not)
"""
from transport.sql.common import Base, BaseReader, BaseWriter

class Duck :
    def __init__(self,**_args):
        #
        # duckdb with none as database will operate as an in-memory database
        #
        self.database = _args['database'] if 'database' in _args else ''
    def get_provider(self):
        return "duckdb"
    
    def _get_uri(self,**_args):
        return f"""duckdb:///{self.database}"""
class Reader(Duck,BaseReader) :
    def __init__(self,**_args):
        Duck.__init__(self,**_args)
        BaseReader.__init__(self,**_args)
class Writer(Duck,BaseWriter):
    def __init__(self,**_args):
        Duck.__init__(self,**_args)
        BaseWriter.__init__(self,**_args)
