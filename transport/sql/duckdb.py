"""
This module implements the handler for duckdb (in memory or not)
"""
from transport.sql.common import Base, BaseReader, BaseWriter

class Duck :
    def __init__(self,**_args):
        self.database = _args['database']
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
