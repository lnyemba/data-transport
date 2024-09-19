"""
This file encapsulates common operations associated with SQL databases via SQLAlchemy

"""
import sqlalchemy as sqa
from sqlalchemy import text 

import pandas as pd

class Base:
    def __init__(self,**_args):
        self._host = _args['host'] if 'host' in _args else 'localhost'
        self._port = None
        self._database = _args['database']
        self._table = _args['table'] if 'table' in _args else None
        self._engine= sqa.create_engine(self._get_uri(**_args),future=True)
    def _set_uri(self,**_args) :
        """
        :provider   provider
        :host       host and port
        :account    account user/pwd
        """
        _account = _args['account'] if 'account' in _args else  None
        _host   = _args['host']
        _provider = _args['provider'].replace(':','').replace('/','').strip()
    def _get_uri(self,**_args):
        """
        This function will return the formatted uri for the sqlAlchemy engine
        """
        raise Exception ("Function Needs to be implemented ")
    def meta (self,**_args):
        """
        This function returns the schema (table definition) of a given table
        :table  optional name of the table (can be fully qualified)
        """
        _table = self._table if 'table' not in _args else _args['table']
        _schema = []
        if _table :
            if sqa.__version__.startswith('1.') :
                _handler = sqa.MetaData(bind=self._engine)
                _handler.reflect()
            else:
                #
                # sqlalchemy's version 2.+
                _handler = sqa.MetaData()
                _handler.reflect(bind=self._engine)
            #
            # Let us extract the schema with the native types
            _map = {'BIGINT':'INTEGER','TEXT':'STRING','DOUBLE_PRECISION':'FLOAT','NUMERIC':'FLOAT','DECIMAL':'FLOAT','REAL':'FLOAT'}
            _schema = [{"name":_attr.name,"type":_map.get(str(_attr.type),str(_attr.type))} for _attr in _handler.tables[_table].columns]
        return _schema
    def  has(self,**_args):
        return self.meta(**_args)
    def apply(self,sql):
        """
        Executing sql statement that returns query results (hence the restriction on sql and/or with)
        :sql    SQL query to be exectued

        @TODO: Execution of stored procedures
        """
        if sql.lower().startswith('select') or sql.lower().startswith('with') :

            return pd.read_sql(sql,self._engine) 
        else:
            _handler = self._engine.connect()
            _handler.execute(text(sql))
            _handler.commit ()
            _handler.close()
        return None

class SQLBase(Base):
    def __init__(self,**_args):
        super().__init__(**_args)
    def get_provider(self):
        raise Exception ("Provider Needs to be set ...")
    def get_default_port(self) :
        raise Exception ("default port needs to be set")
    
    def _get_uri(self,**_args):
        _host = self._host 
        _account = ''
        if self._port :
            _port = self._port
        else:
            _port = self.get_default_port()

        _host = f'{_host}:{_port}'
        
        if 'username' in _args :
            _account = ''.join([_args['username'],':',_args['password'],'@'])
        _database = self._database
        _provider = self.get_provider().replace(':','').replace('/','')
        # _uri = [f'{_provider}:/',_account,_host,_database]
        # _uri = [_item.strip() for _item in _uri if _item.strip()]
        # return '/'.join(_uri)
        return f'{_provider}://{_host}/{_database}' if _account == '' else f'{_provider}://{_account}{_host}/{_database}'

class BaseReader(SQLBase):
    def __init__(self,**_args):
        super().__init__(**_args)    
    def read(self,**_args):
        """
        This function will read a query or table from the specific database
        """
        if 'sql' in _args :
            sql = _args['sql']
        else:
            _table = _args['table'] if 'table' in _args else self._table
            sql = f'SELECT * FROM {_table}'
        return self.apply(sql)
    

class BaseWriter (SQLBase):
    """
    This class implements SQLAlchemy support for Writting to a data-store (RDBMS)
    """
    def __init__(self,**_args):
        super().__init__(**_args)
    def write(self,_data,**_args):
        if type(_data) == dict :
            _df = pd.DataFrame(_data)
        elif type(_data) == list :
            _df = pd.DataFrame(_data)
        else:
            _df = _data.copy()
        #
        # We are assuming we have a data-frame at this point
        #
        _table = _args['table'] if 'table' in _args else self._table
        _mode = {'chunksize':2000000,'if_exists':'append','index':False}
        for key in ['if_exists','index','chunksize'] :
            if key in _args :
                _mode[key] = _args[key]
        # if 'schema' in _args :
        #     _mode['schema'] = _args['schema']
        # if 'if_exists' in _args :
        #     _mode['if_exists'] = _args['if_exists']

        _df.to_sql(_table,self._engine,**_mode)