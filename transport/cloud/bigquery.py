"""
Implementing support for google's bigquery
    - cloud.bigquery.Read
    - cloud.bigquery.Write
"""
import json
from google.oauth2 import service_account
from google.cloud import bigquery as bq

from multiprocessing import Lock, RLock
import pandas as pd
import pandas_gbq as pd_gbq
import numpy as np
import time

MAX_CHUNK = 2000000
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
        table = _args['table'] if 'table' in _args else self.table
        
        try:
            if table :
                _dataset = self.dataset if 'dataset' not in _args else _args['dataset']
                sql = f"""SELECT column_name as name, data_type as type FROM {_dataset}.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{table}' """
                _info = {'credentials':self.credentials,'dialect':'standard'}   
                return pd_gbq.read_gbq(sql,**_info).to_dict(orient='records')
                # return self.read(sql=sql).to_dict(orient='records')
                # ref     = self.client.dataset(self.dataset).table(table)
                
                # _schema =  self.client.get_table(ref).schema
                # return [{"name":_item.name,"type":_item.field_type,"description":( "" if not hasattr(_item,"description") else _item.description )} for _item in _schema]
            else :
                return []
        except Exception as e:
            
            return []
    def has(self,**_args):
        found = False
        try:
            _has = self.meta(**_args)
            found = _has is not None and len(_has) > 0 
        except Exception as e:
            pass
        return found
class Reader (BigQuery):
    """
    Implementing support for reading from bigquery, This class acts as a wrapper around google's API
    """
    def __init__(self,**_args):
        
        super().__init__(**_args)    
    def apply(self,sql):
        return self.read(sql=sql)
        
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
        return pd_gbq.read_gbq(SQL,**_info) if SQL else None  
        # return self.client.query(SQL).to_dataframe() if SQL else None
 
class Writer (BigQuery):
    """
    This class implements support for writing against bigquery
    """
    lock = RLock()
    def __init__(self,**_args):
        super().__init__(**_args)    
        
        self.parallel = False if 'lock' not in _args else _args['lock']
        self.table = _args['table'] if 'table' in _args else None
        self.mode = {'if_exists':'append','chunksize':900000,'destination_table':self.table,'credentials':self.credentials}
        self._chunks = 1 if 'chunks' not in _args else int(_args['chunks'])
        self._location = 'US' if 'location' not in _args else _args['location']
    def write(self,_data,**_args) :
        """
        This function will perform a write to bigquery
        :_data  data-frame to be written to bigquery
        """
        try:
            if self.parallel or 'lock' in _args :
                Write.lock.acquire()
            _args['table'] = self.table if 'table' not in _args else _args['table']
            self._write(_data,**_args)
        finally:
            if self.parallel:
                Write.lock.release()
    def submit(self,_sql):
        """
        Write the output of a massive query to a given table, biquery will handle this as a job
        This function will return the job identifier
        """
        _config = bq.QueryJobConfig()
        _config.destination = self.client.dataset(self.dataset).table(self.table)
        _config.allow_large_results = True
        # _config.write_disposition = bq.bq_consts.WRITE_APPEND     
        _config.dry_run = False
        # _config.priority = 'BATCH'   
        _resp = self.client.query(_sql,location=self._location,job_config=_config)
        return _resp.job_id
    def status (self,_id):
        return self.client.get_job(_id,location=self._location)
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
                #
                # Let us insure that the types are somewhat compatible ...
                # _map = {'INTEGER':np.int64,'DATETIME':'datetime64[ns]','TIMESTAMP':'datetime64[ns]','FLOAT':np.float64,'DOUBLE':np.float64,'STRING':str}
            # _mode = copy.deepcopy(self.mode)
            _mode = self.mode
            # _df.to_gbq(**self.mode) #if_exists='append',destination_table=partial,credentials=credentials,chunksize=90000)	
            #
            # Let us adjust the chunking here 
            self._chunks = 10 if _df.shape[0] > MAX_CHUNK and self._chunks == 1 else self._chunks 
            _indexes = np.array_split(np.arange(_df.shape[0]),self._chunks) 
            for i in _indexes :
                _df.iloc[i].to_gbq(**self.mode)
                time.sleep(1)
        pass    