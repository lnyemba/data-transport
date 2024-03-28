import sqlalchemy
import pandas as pd
from transport.sql.common import Base, BaseReader, BaseWriter
class SQLite (BaseReader):
    def __init__(self,**_args):
        super().__init__(**_args)
        if 'path' in _args :
            self._database = _args['path']
        if 'database' in _args :
            self._database = _args['database']
    def _get_uri(self,**_args):
        path = self._database
        return f'sqlite:///{path}' # ensure this is the correct path for the sqlite file. 

class Reader(SQLite,BaseReader):
    def __init__(self,**_args):
        super().__init__(**_args)
    # def read(self,**_args):
    #     sql = _args['sql']
    #     return pd.read_sql(sql,self._engine)


class Writer (SQLite,BaseWriter):
    def __init__(self,**_args):
        super().__init__(**_args)