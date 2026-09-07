"""
This module implements the handler for duckdb (in memory or not)
"""
from transport.sql.common import Base, BaseReader, BaseWriter
def template ():
    return {'database':'path-to-database','table':'table'}
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
    def _get_uri(self,**_args):
        #
        # if we are working with an in-memory database we can NOT set the attributes to be read-only
        # something to do with SQL-Alchemy
        p = self.database.strip().startswith(":") and self.database.strip().endswith(":")
        if not p :
            return super()._get_uri(**_args),{'connect_args':{'read_only':True}}
        else:
            #
            # we have an in-memory database 
            return super()._get_uri(**_args),{}
class Writer(Duck,BaseWriter):
    def __init__(self,**_args):
        Duck.__init__(self,**_args)
        BaseWriter.__init__(self,**_args)
