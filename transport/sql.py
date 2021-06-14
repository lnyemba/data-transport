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
if sys.version_info[0] > 2 : 
    from transport.common import Reader, Writer #, factory
else:
	from common import Reader,Writer
import json
from google.oauth2 import service_account
from google.cloud import bigquery as bq
from multiprocessing import Lock
import pandas as pd
import numpy as np
import copy


class SQLRW :
    PROVIDERS = {"postgresql":"5432","redshift":"5432","mysql":"3306","mariadb":"3306"}
    DRIVERS  = {"postgresql":pg,"redshift":pg,"mysql":my,"mariadb":my}
    def __init__(self,**_args):
        
        
        _info = {}
        _info['dbname']     = _args['db']
        self.table      = _args['table']
        self.fields     = _args['fields'] if 'fields' in _args else []
        
        if 'host' in _args :
            _info['host'] = 'localhost' if 'host' not in _args else _args['host']
            _info['port'] = SQLWriter.PROVIDERS[_args['provider']] if 'port' not in _args else _args['port']
        
        if 'username' in _args or 'user' in _args:
            key = 'username' if 'username' in _args else 'user'
            _info['user'] = _args[key]
            _info['password'] = _args['password']
        #
        # We need to load the drivers here to see what we are dealing with ...
        _handler = SQLWriter.DRIVERS[_args['provider']]
        self.conn = _handler.connect(**_info)
    
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
                cursor.close()
                return pd.read_sql(_sql,self.conn)
            else:
                # Executing a command i.e no expected return values ...
                cursor.execute(_sql)
                self.conn.commit()
            
        finally:
            self.conn.commit()
            cursor.close()
    def close(self):
        try:
            self.connect.close()
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
            _sql = "SELECT :fields FROM "+self.table
            if 'filter' in _args :
                _sql = _sql +" WHERE "+_args['filter']
            _fields = '*' if not self.fields else ",".join(self.fields) 
            _sql = _sql.replace(":fields",_fields)
        if 'limit' in _args :
            _sql = _sql + " LIMIT "+str(_args['limit'])
        return self.apply(_sql)

class SQLWriter(SQLRW,Writer):
    def __init__(self,**_args) :
        super().__init__(**_args)    
        #
        # In the advent that data typing is difficult to determine we can inspect and perform a default case
        # This slows down the process but improves reliability of the data
        # NOTE: Proper data type should be set on the target system if their source is unclear.
        self._inspect = False if 'inspect' not in _args else _args['inspect']
        self._cast = False if 'cast' not in _args else _args['cast']
    def init(self,fields):
        if not fields :
            try:                
                self.fields = pd.read_sql("SELECT * FROM :table LIMIT 1".replace(":table",self.table),self.conn).columns.tolist()
            finally:
                pass
        else:
            self.fields = fields;

    def make(self,fields):
        self.fields = fields
        sql = " ".join(["CREATE TABLE",self.table," (", ",".join(fields),")"])
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql)
        except Exception as e :
            pass
        finally:
            cursor.close()
    def write(self,info):
        """
        :param info writes a list of data to a given set of fields
        """
        # inspect = False if 'inspect' not in _args else _args['inspect']
        # cast = False if 'cast' not in _args else _args['cast']
        if not self.fields :
            _fields = info.keys() if type(info) == dict else info[0].keys()
            _fields = list (_fields)
            self.init(_fields)

        if type(info) != list :
            info = [info]        
        cursor = self.conn.cursor()
        try:
            _sql = "INSERT INTO :table (:fields) values (:values)".replace(":table",self.table) #.replace(":table",self.table).replace(":fields",_fields)
            if self._inspect :
                for _row in info :
                    fields = list(_row.keys())
                    if self._cast == False :
                        values = ",".join(_row.values())
                    else:
                        # values = "'"+"','".join([str(value) for value in _row.values()])+"'"
                        values = [",".join(["%(",name,")s"]) for name in _row.keys()]
                    
                    # values = [ "".join(["'",str(_row[key]),"'"]) if np.nan(_row[key]).isnumeric() else str(_row[key]) for key in _row]
                    # print (values)
                    query = _sql.replace(":fields",",".join(fields)).replace(":values",values)
                    
                    cursor.execute(query,_row.values())
                

                pass
            else:
                _fields = ",".join(self.fields)
                _sql = _sql.replace(":fields",_fields)
                _sql = _sql.replace(":values",",".join(["%("+name+")s" for name in self.fields]))
                
                # for row in info :
                #     values = ["'".join(["",value,""]) if not str(value).isnumeric() else value for value in row.values()]
                cursor.executemany(_sql,info)   
            # self.conn.commit()
        except Exception as e:
            print (e) 
        finally:
            self.conn.commit()
            cursor.close()
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
    def meta(self,**_args):
        """
        This function returns meta data for a given table or query with dataset/table properly formatted
        :param table    name of the name WITHOUT including dataset
        :param sql      sql query to be pulled,
        """
        #if 'table' in _args :
        #    sql = "SELECT * from :dataset."+ _args['table']" limit 1"
        #else:
        #    sql = _args['sql'] 
        #    if 'limit' not in sql.lower() :
        #        sql = sql + ' limit 1'

        #sql = sql.replace(':dataset',self.dataset) if ':dataset' in args else sql

        #
        # Let us return the schema information now for a given table 
        #
        table = _args['table']
        client = bq.Client.from_service_account_json(self.path)
        ref     = client.dataset(self.dataset).table(table)
        return client.get_table(ref).schema
class BQReader(BigQuery,Reader) :
    def __init__(self,**_args):
        
        super().__init__(**_args)    

        pass
    def read(self,**_args):
        SQL = None
        if 'sql' in _args :
            SQL = _args['sql']
        elif 'table' in _args:

            table = "".join(["`",_args['table'],"`"]) 
            SQL = "SELECT  * FROM :table ".replace(":table",table)
        if SQL and 'limit' in _args:
            SQL += " LIMIT "+str(_args['limit'])
        if (':dataset' in SQL or ':DATASET' in SQL)  and self.dataset:
            SQL = SQL.replace(':dataset',self.dataset).replace(':DATASET',self.dataset)
        _info = {'credentials':self.credentials,'dialect':'standard'}
        if 'dtypes' in _args  or self.dtypes :
            self.dtypes = _args ['dtypes'] if 'dtypes' in self.dtypes else None
            if self.dtypes :
                _info['dtypes'] = self.dtypes
        return pd.read_gbq(SQL,*_info) if SQL else None    
        # return pd.read_gbq(SQL,credentials=self.credentials,dialect='standard') if SQL else None
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
            _mode = copy.deepcopy(self.mode)
            _df.to_gbq(**self.mode) #if_exists='append',destination_table=partial,credentials=credentials,chunksize=90000)	
            
        pass
# import transport    
# reader = transport.factory.instance(type="sql.BQReader",args={"service_key":"/home/steve/dev/google-cloud-sdk/accounts/curation-prod.json"})
# _df = reader.read(sql="select  * from `2019q1r4_combined.person` limit 10")
# writer = transport.factory.instance(type="sql.BQWriter",args={"service_key":"/home/steve/dev/google-cloud-sdk/accounts/curation-prod.json"})
# writer.write(_df,table='2019q1r4_combined.foo')
# write.write()
# _args = {"db":"sample","table":"foo","provider":"postgresql"}
# # # w = SQLWriter(**_args)
# # # w.write({"name":"kalara.io","email":"ceo@kalara.io","age":10})
# r = SQLReader(**_args)
# print (r.read(filter='age > 0',limit = 20))
