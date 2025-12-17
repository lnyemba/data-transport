import sqlalchemy
import pandas as pd
from .. sql.common import BaseReader , BaseWriter
import sqlalchemy as sqa


def template():
    return {'host':'localhost','port':8047,'ssl':False,'table':None,'database':None}

class Drill :
    __template = {'host':None,'port':None,'ssl':None,'table':None,'database':None}
    def __init__(self,**_args):

        self._host = _args['host'] if 'host' in _args else 'localhost'
        self._port = _args['port'] if 'port' in _args else self.get_default_port()
        self._ssl = False if 'ssl' not in _args else _args['ssl']
        
        self._table = _args['table'] if 'table' in _args else None
        if self._table and '.' in self._table :
            _seg = self._table.split('.')
            if len(_seg) > 2 :
                self._schema,self._database = _seg[:2]
        else:
            
            self._database=_args['database']
            self._schema = self._database.split('.')[0]
        
    def _get_uri(self,**_args):
        return f'drill+sadrill://{self._host}:{self._port}/{self._database}?use_ssl={self._ssl}'
    def get_provider(self):
        return "drill+sadrill"
    def get_default_port(self):
        return "8047"
    def meta(self,**_args):
        _table = _args['table'] if 'table' in _args else self._table
        if '.' in _table :
            _schema = _table.split('.')[:2]
            _schema = '.'.join(_schema)
            _table = _table.split('.')[-1]
        else:
            _schema = self._schema
        
        # _sql = f"select COLUMN_NAME AS name, CASE WHEN DATA_TYPE ='CHARACTER VARYING' THEN 'CHAR ( 125 )' ELSE DATA_TYPE END AS type from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{_schema}' and TABLE_NAME='{_table}'"
        _sql = f"select COLUMN_NAME AS name, CASE WHEN DATA_TYPE ='CHARACTER VARYING' THEN 'CHAR ( '||COLUMN_SIZE||' )' ELSE DATA_TYPE END AS type from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{_schema}' and TABLE_NAME='{_table}'"
        try:
            _df  = pd.read_sql(_sql,self._engine)
            return _df.to_dict(orient='records')
        except Exception as e:
            print (e)
            pass
        return []
class Reader (Drill,BaseReader) :
    def __init__(self,**_args):
        super().__init__(**_args)
        self._chunksize = 0 if 'chunksize' not in _args else _args['chunksize']
        self._engine= sqa.create_engine(self._get_uri(),future=True)
class Writer(Drill,BaseWriter):
    def __init__(self,**_args):
        super().__init__(self,**_args)