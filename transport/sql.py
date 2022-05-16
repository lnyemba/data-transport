"""
This file is intended to perform read/writes against an SQL database such as PostgreSQL, Redshift, Mysql, MsSQL ...

LICENSE (MIT)
Copyright 2016-2020, The Phi Technology LLC

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""
import psycopg2 as pg
import mysql.connector as my
import sys

import sqlalchemy
if sys.version_info[0] > 2 : 
    from transport.common import Reader, Writer #, factory
else:
	from common import Reader,Writer
import json
from google.oauth2 import service_account
from google.cloud import bigquery as bq
from multiprocessing import Lock, RLock
import pandas as pd
import numpy as np
import nzpy as nz   #--- netezza drivers
import copy
import os


class SQLRW :
    lock = RLock()
    DRIVERS  = {"postgresql":pg,"redshift":pg,"mysql":my,"mariadb":my,"netezza":nz}
    REFERENCE = {
        "netezza":{"port":5480,"handler":nz,"dtype":"VARCHAR(512)"},
        "postgresql":{"port":5432,"handler":pg,"dtype":"VARCHAR"},
        "redshift":{"port":5432,"handler":pg,"dtype":"VARCHAR"},
        "mysql":{"port":3360,"handler":my,"dtype":"VARCHAR(256)"},
        "mariadb":{"port":3360,"handler":my,"dtype":"VARCHAR(256)"},
        }
    def __init__(self,**_args):
        
        
        _info = {}
        _info['dbname'] = _args['db'] if 'db' in _args else _args['database']
        self.table      = _args['table'] if 'table' in _args else None
        self.fields     = _args['fields'] if 'fields' in _args else []
        self.schema = _args['schema'] if 'schema' in _args else ''
        
        self._provider       = _args['provider'] if 'provider' in _args else None
        # _info['host'] = 'localhost' if 'host' not in _args else _args['host']
        # _info['port'] = SQLWriter.REFERENCE[_provider]['port'] if 'port' not in _args else _args['port']

        _info['host'] = _args['host']
        _info['port'] = _args['port']
        
        # if 'host' in _args :
        #     _info['host'] = 'localhost' if 'host' not in _args else _args['host']
        #     # _info['port'] = SQLWriter.PROVIDERS[_args['provider']] if 'port' not in _args else _args['port']
        #     _info['port'] = SQLWriter.REFERENCE[_provider]['port'] if 'port' not in _args else _args['port']
        
        if 'username' in _args or 'user' in _args:
            key = 'username' if 'username' in _args else 'user'
            _info['user'] = _args[key]
            _info['password'] = _args['password'] if 'password' in _args else ''
        if 'auth_file' in _args :
            _auth = json.loads( open(_args['auth_file']).read() )
            key = 'username' if 'username' in _auth else 'user'
            _info['user'] = _auth[key]
            _info['password'] = _auth['password'] if 'password' in _auth else ''

            _info['host'] = _auth['host'] if 'host' in _auth else _info['host']
            _info['port'] = _auth['port'] if 'port' in _auth else _info['port']
            if 'database' in _auth:
                _info['dbname'] = _auth['database']
            self.table = _auth['table'] if 'table' in _auth else self.table
        #
        # We need to load the drivers here to see what we are dealing with ...
        
        
        # _handler = SQLWriter.REFERENCE[_provider]['handler']
        _handler        = _args['driver']  #-- handler to the driver
        self._dtype     = _args['default']['type'] if 'default' in _args and 'type' in _args['default'] else 'VARCHAR(256)'
        # self._provider  = _args['provider']
        # self._dtype = SQLWriter.REFERENCE[_provider]['dtype'] if 'dtype' not in _args else _args['dtype']
        # self._provider = _provider
        if _handler == nz :
            _info['database'] = _info['dbname']
            _info['securityLevel'] = 0
            del _info['dbname']
        if _handler == my :
            _info['database'] = _info['dbname']
            del _info['dbname']
        
        self.conn = _handler.connect(**_info)
        self._engine = _args['sqlalchemy']  if 'sqlalchemy' in _args else None
    def meta(self,**_args):
        return []
    def _tablename(self,name) :
        
        return self.schema +'.'+name if self.schema not in [None, ''] and '.' not in name else name 
    def has(self,**_args):
        found = False
        try:
            
            table = self._tablename(_args['table'])if 'table' in _args else self._tablename(self.table)
            sql = "SELECT * FROM :table LIMIT 1".replace(":table",table)
            if self._engine :
                _conn = self._engine.connect()
            else:
                _conn = self.conn
            found = pd.read_sql(sql,_conn).shape[0] 
            found = True

        except Exception as e:
            pass
        finally:
            if self._engine :
                _conn.close()
        return found
    def isready(self):
        _sql = "SELECT * FROM :table LIMIT 1".replace(":table",self.table)
        try:
            return pd.read_sql(_sql,self.conn).columns.tolist()
        except Exception as e:
            pass
        return False
    def apply(self,_sql):
        """
        This function applies a command and/or a query against the current relational data-store
        :param _sql     insert/select statement
        @TODO: Store procedure calls
        """
        cursor = self.conn.cursor()
        _out = None
        try:
            if "select" in _sql.lower() :
                
                # _conn = self._engine if self._engine else self.conn
                return pd.read_sql(_sql,self.conn)
            else:
                # Executing a command i.e no expected return values ...
                cursor.execute(_sql)
                self.conn.commit()
        except Exception as e :
            print (e)    
        finally:
            self.conn.commit()
            cursor.close()
    def close(self):
        try:
            self.conn.close()
        except Exception as error :
            print (error)
            pass
class SQLReader(SQLRW,Reader) :
    def __init__(self,**_args):
        super().__init__(**_args) 
        
    def read(self,**_args):
        if 'sql' in _args :            
            _sql = (_args['sql'])
        else:
            table = self.table if self.table is not None else _args['table']
            _sql = "SELECT :fields FROM "+self._tablename(table)
            if 'filter' in _args :
                _sql = _sql +" WHERE "+_args['filter']
            _fields = '*' if not self.fields else ",".join(self.fields) 
            _sql = _sql.replace(":fields",_fields)
        if 'limit' in _args :
            _sql = _sql + " LIMIT "+str(_args['limit'])
        return self.apply(_sql)
    def close(self) :
        try:
            self.conn.close()
        except Exception as error :
            print (error)
            pass

class SQLWriter(SQLRW,Writer):
    def __init__(self,**_args) :
        super().__init__(**_args)    
        #
        # In the advent that data typing is difficult to determine we can inspect and perform a default case
        # This slows down the process but improves reliability of the data
        # NOTE: Proper data type should be set on the target system if their source is unclear.
        
        self._cast = False if 'cast' not in _args else _args['cast']
        
    def init(self,fields=None):
        if not fields :
            try:                
                table = self._tablename(self.table)
                self.fields = pd.read_sql_query("SELECT * FROM :table LIMIT 1".replace(":table",table),self.conn).columns.tolist()
            finally:
                pass
        else:
            self.fields = fields;

    def make(self,**_args):
        table = self._tablename(self.table) if 'table' not in _args else self._tablename(_args['table'])
        if 'fields' in _args :
            fields = _args['fields']  
            # table = self._tablename(self.table)          
            sql = " ".join(["CREATE TABLE",table," (", ",".join([ name +' '+ self._dtype for name in fields]),")"])
            
        else:
            schema = _args['schema'] if 'schema' in _args else []
            
            _map = _args['map'] if 'map' in _args else {}
            sql = [] # ["CREATE TABLE ",_args['table'],"("]
            for _item in schema :
                _type = _item['type']
                if _type in _map :
                    _type = _map[_type]
                sql = sql + [" " .join([_item['name'], ' ',_type])]
            sql = ",".join(sql)
            # table = self._tablename(_args['table'])
            sql = ["CREATE TABLE ",table,"( ",sql," )"]
            sql = " ".join(sql)
            
        cursor = self.conn.cursor()
        try:
            
            cursor.execute(sql)
        except Exception as e :
            print (e)
            # print (sql)
            pass
        finally:
            # cursor.close()
            self.conn.commit()
            pass
    def write(self,info,**_args):
        """
        :param info writes a list of data to a given set of fields
        """
        # inspect = False if 'inspect' not in _args else _args['inspect']
        # cast = False if 'cast' not in _args else _args['cast']
        if not self.fields :
            if type(info) == list :
                _fields = info[0].keys()
            elif type(info) == dict :
                _fields = info.keys()
            elif type(info) == pd.DataFrame :
                _fields = info.columns.tolist()

            # _fields = info.keys() if type(info) == dict else info[0].keys()
            _fields = list (_fields)
            self.init(_fields)
        #
        # @TODO: Use pandas/odbc ? Not sure b/c it requires sqlalchemy
        #
        # if type(info) != list :
        #     #
        #     # We are assuming 2 cases i.e dict or pd.DataFrame
        #     info = [info]  if type(info) == dict else info.values.tolist()       
        
        try:
            table = self._tablename(self.table)
            _sql = "INSERT INTO :table (:fields) VALUES (:values)".replace(":table",table) #.replace(":table",self.table).replace(":fields",_fields)
           
            if type(info) == list :
                _info = pd.DataFrame(info)
            elif type(info) == dict :
                _info = pd.DataFrame([info])
            else:
                _info = pd.DataFrame(info)
        
            
            if _info.shape[0] == 0 :
                
                return
            SQLRW.lock.acquire()
            
            if self._engine is not None:
                # pd.to_sql(_info,self._engine)
                if self.schema in ['',None] :
                    rows = _info.to_sql(table,self._engine,if_exists='append',index=False)
                else:
                    rows = _info.to_sql(self.table,self._engine,schema=self.schema,if_exists='append',index=False)
                
            else:
                _fields = ",".join(self.fields)
                _sql = _sql.replace(":fields",_fields)
                values = ", ".join("?"*len(self.fields)) if self._provider == 'netezza' else ",".join(["%s" for name in self.fields])
                _sql = _sql.replace(":values",values)
                cursor = self.conn.cursor()
                cursor.executemany(_sql,_info.values.tolist())  
                cursor.close()
            # cursor.commit() 
            
            # self.conn.commit()
        except Exception as e:
            print(e)
            pass
        finally:
            
            if self._engine is None :
                self.conn.commit()   
            SQLRW.lock.release()         
            # cursor.close()
            pass
    def close(self):
        try:
            self.conn.close()
        finally:
            pass
class BigQuery:
    def __init__(self,**_args):
        path = _args['service_key'] if 'service_key' in _args else _args['private_key']
        self.credentials = service_account.Credentials.from_service_account_file(path)
        self.dataset = _args['dataset'] if 'dataset' in _args else None
        self.path = path
        self.dtypes = _args['dtypes'] if 'dtypes' in _args else None
        self.table = _args['table'] if 'table' in _args else None
        self.client = bq.Client.from_service_account_json(self.path)
    def meta(self,**_args):
        """
        This function returns meta data for a given table or query with dataset/table properly formatted
        :param table    name of the name WITHOUT including dataset
        :param sql      sql query to be pulled,
        """
        table = _args['table'] 
        try:
            ref     = self.client.dataset(self.dataset).table(table)
            return self.client.get_table(ref).schema
        except Exception as e:
            return []
    def has(self,**_args):
        found = False
        try:
            found = self.meta(**_args) is not None
        except Exception as e:
            pass
        return found
class BQReader(BigQuery,Reader) :
    def __init__(self,**_args):
        
        super().__init__(**_args)    
    def apply(self,sql):
        self.read(sql=sql)
        pass
    def read(self,**_args):
        SQL = None
        table = self.table if 'table' not in _args else _args['table']
        if 'sql' in _args :
            SQL = _args['sql']
        elif table:

            table = "".join(["`",table,"`"]) if '.' in table else "".join(["`:dataset.",table,"`"])
            SQL = "SELECT  * FROM :table ".replace(":table",table)
        if not SQL :
            return None
        if SQL and 'limit' in _args:
            SQL += " LIMIT "+str(_args['limit'])
        if (':dataset' in SQL or ':DATASET' in SQL)  and self.dataset:
            SQL = SQL.replace(':dataset',self.dataset).replace(':DATASET',self.dataset)
        _info = {'credentials':self.credentials,'dialect':'standard'}       
        return pd.read_gbq(SQL,**_info) if SQL else None  
        # return self.client.query(SQL).to_dataframe() if SQL else None
        

class BQWriter(BigQuery,Writer):
    lock = Lock()
    def __init__(self,**_args):
        super().__init__(**_args)    
        
        self.parallel = False if 'lock' not in _args else _args['lock']
        self.table = _args['table'] if 'table' in _args else None
        self.mode = {'if_exists':'append','chunksize':900000,'destination_table':self.table,'credentials':self.credentials}

    def write(self,_info,**_args) :
        try:
            if self.parallel or 'lock' in _args :
                BQWriter.lock.acquire()
            _args['table'] = self.table if 'table' not in _args else _args['table']
            self._write(_info,**_args)
        finally:
            if self.parallel:
                BQWriter.lock.release()
    def _write(self,_info,**_args) :
        _df = None
        if type(_info) in [list,pd.DataFrame] :
            if type(_info) == list :
                _df = pd.DataFrame(_info)
            elif type(_info) == pd.DataFrame :
                _df = _info 
            
            if '.'  not in _args['table'] :
                self.mode['destination_table'] = '.'.join([self.dataset,_args['table']])
            else:

                self.mode['destination_table'] = _args['table'].strip()
            if 'schema' in _args :
                self.mode['table_schema'] = _args['schema']
            # _mode = copy.deepcopy(self.mode)
            _mode = self.mode
            _df.to_gbq(**self.mode) #if_exists='append',destination_table=partial,credentials=credentials,chunksize=90000)	
            
        pass
#
# Aliasing the big query classes allowing it to be backward compatible
#
BigQueryReader = BQReader
BigQueryWriter = BQWriter