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
# from threading import Lock

import pandas as pd

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
        try:
            if "insert" in _sql .lower() or "update" in _sql.lower() :
                # Executing a command i.e no expected return values ...
                cursor.execute(_sql)
            else:
                cursor.close()
                return pd.read_sql(_sql,self.conn)
            
        finally:
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
        if not self.fields :
            _fields = info.keys() if type(info) == dict else info[0].keys()
            _fields = list (_fields)
            self.init(_fields)

        if type(info) != list :
            info = [info]        
        cursor = self.conn.cursor()
        try:
            
            _fields = ",".join(self.fields)
            _sql = "INSERT INTO :table (:fields) values (:values)".replace(":table",self.table).replace(":fields",_fields)
            _sql = _sql.replace(":values",",".join(["%("+name+")s" for name in self.fields]))
            
            # for row in info :
            #     values = ["'".join(["",value,""]) if not str(value).isnumeric() else value for value in row.values()]
            cursor.executemany(_sql,info)   
            self.conn.commit()
        except Exception as e:
            print (e) 
        finally:
            cursor.close()
            pass
    def close(self):
        try:
            self.conn.close()
        finally:
            pass

# _args = {"db":"sample","table":"foo","provider":"postgresql"}
# # # w = SQLWriter(**_args)
# # # w.write({"name":"kalara.io","email":"ceo@kalara.io","age":10})
# r = SQLReader(**_args)
# print (r.read(filter='age > 0',limit = 20))