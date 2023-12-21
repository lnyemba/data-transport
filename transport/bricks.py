"""
This file implements databricks handling, This functionality will rely on databricks-sql-connector
LICENSE (MIT)
Copyright 2016-2020, The Phi Technology LLC

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


@TODO:
    - Migrate SQLite to SQL hierarchy
    - Include Write in Chunks from pandas
"""
import os
import sqlalchemy
from transport.common import Reader,Writer
import pandas as pd


class Bricks:
    """
    :host
    :token
    :database
    :cluster_path
    :table
    """
    def __init__(self,**_args):
        _host = _args['host']
        _token= _args['token']
        _cluster_path = _args['cluster_path']
        self._schema = _args['schema'] if 'schema' in _args else _args['database']
        _catalog = _args['catalog']
        self._table = _args['table'] if 'table' in _args else None

        #
        # @TODO:
        # Sometimes when the cluster isn't up and running it takes a while, the user should be alerted of this
        #

        _uri = f'''databricks://token:{_token}@{_host}?http_path={_cluster_path}&catalog={_catalog}&schema={self._schema}'''
        self._engine = sqlalchemy.create_engine (_uri)
        pass
    def meta(self,**_args):
        table = _args['table'] if 'table' in _args else self._table
        if not table :
            return []
        else:
            if sqlalchemy.__version__.startswith('1.') :
                _m = sqlalchemy.MetaData(bind=self._engine)
                _m.reflect(only=[table])
            else:
                _m = sqlalchemy.MetaData()
                _m.reflect(bind=self._engine)
            #
            # Let's retrieve te information associated with a table
            # 
            return [{'name':_attr.name,'type':_attr.type} for _attr in _m.tables[table].columns]  

    def has(self,**_args):
        return self.meta(**_args)
    def apply(self,_sql):
        try:
            if _sql.lower().startswith('select') :
                return pd.read_sql(_sql,self._engine)
        except Exception as e:
            pass

class BricksReader(Bricks,Reader):
    """
    This class is designed for reads and will execute reads against a table name or a select SQL statement
    """
    def __init__(self,**_args):
        super().__init__(**_args)
    def read(self,**_args):
        limit = None if 'limit' not in _args else str(_args['limit'])
        
        if 'sql' in _args :
            sql = _args['sql']
        elif 'table' in _args  :
            table = _args['table']
            sql = f'SELECT * FROM {table}'
        if limit :
            sql = sql + f' LIMIT {limit}'

        if 'sql' in _args or 'table' in _args :            
            return self.apply(sql)
        else:
            return pd.DataFrame() 
    pass
class BricksWriter(Bricks,Writer):
    def __init__(self,**_args):
        super().__init__(**_args)
    def write(self,_data,**_args):  
        """
        This data will write data to data-bricks against a given table. If the table is not specified upon initiazation, it can be specified here
        _data:  data frame to push to databricks
        _args:  chunks, table, schema
        """    
        _schema = self._schema if 'schema' not in _args else _args['schema']
        _table = self._table if 'table' not in _args else _args['table']
        _df = _data if type(_data) == pd.DataFrame else _data
        if type(_df) == dict :
            _df = [_df]
        if type(_df) == list :
            _df = pd.DataFrame(_df)
        _df.to_sql(
            name=_table,schema=_schema,
            con=self._engine,if_exists='append',index=False);
        pass
