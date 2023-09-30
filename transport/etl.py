#!/usr/bin/env python
__doc__ = """
(c) 2018 - 2021 data-transport
steve@the-phi.com, The Phi Technology LLC
https://dev.the-phi.com/git/steve/data-transport.git

This program performs ETL between 9 supported data sources  : Couchdb, Mongodb, Mysql, Mariadb, PostgreSQL, Netezza,Redshift, Sqlite, File
LICENSE (MIT)
Copyright 2016-2020, The Phi Technology LLC

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


Usage :
	transport --config <path-to-file.json> --procs <number-procs>
@TODO: Create tables if they don't exist for relational databases
example of configuration :

1. Move data from a folder to a data-store
	transport [--folder <path> ] --config <config.json>	 #-- assuming the configuration doesn't have folder 
	transport --folder <path> --provider <postgresql|mongo|sqlite> --<database|db> <name> --table|doc <document_name>
In this case the configuration should look like :
	{folder:..., target:{}}
2. Move data from one source to another
	transport --config <file.json>
	{source:{..},target:{..}} or [{source:{..},target:{..}},{source:{..},target:{..}}]
	

"""
import pandas as pd
import numpy as np
import json 
import sys
import transport
import time
import os


from multiprocessing import Process
SYS_ARGS = {}
if len(sys.argv) > 1:
    
    N = len(sys.argv)
    for i in range(1,N):
        value = None
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:] #.replace('-','')
            SYS_ARGS[key] = 1			
            if i + 1 < N:
                value = sys.argv[i + 1] = sys.argv[i+1].strip()
            if key and value and not value.startswith('--'):
                SYS_ARGS[key] = value
                
        
        i += 2
class Transporter(Process):
    """
    The transporter (Jason Stathem) moves data from one persistant store to another
    - callback functions
        :onFinish   callback function when finished
        :onError    callback function when an error occurs
    :source source data specification
    :target destination(s) to move the data to
    """
    def __init__(self,**_args):
        super().__init__()
        # self.onfinish = _args['onFinish'] 
        # self._onerror = _args['onError']
        self._source = _args['source']
        self._target = _args['target']
        
        #
        # Let's insure we can support multiple targets
        self._target = [self._target] if type(self._target) != list else self._target
       
        pass
    def read(self,**_args):
        """
        This function
        """
        _reader = transport.factory.instance(**self._source)
        #
        # If arguments are provided then a query is to be executed (not just a table dump)
        return _reader.read() if 'args' not in self._source else _reader.read(**self._source['args'])
        
    def _delegate_write(self,_data,**_args):
        """
        This function will write a data-frame to a designated data-store, The function is built around a delegation design pattern
        :data   data-frame or object to be written
        """
        if _data.shape[0] > 0 :
            for _target in self._target :
                if 'write' not in _target :
                    _target['context'] = 'write'
                    # _target['lock'] = True
                else:
                    # _target['write']['lock'] = True
                    pass
                _writer = transport.factory.instance(**_target)
                _writer.write(_data.copy(),**_args)
                if hasattr(_writer,'close') :
                    _writer.close()
        
    def write(self,_df,**_args):
        """
        """
        SEGMENT_COUNT = 6
        MAX_ROWS = 1000000
        # _df = self.read()
        _segments = np.array_split(np.range(_df.shape[0]),SEGMENT_COUNT) if _df.shape[0] > MAX_ROWS else np.array( [np.arange(_df.shape[0])])
        # _index = 0
        
        
        for _indexes in _segments :
            _fwd_args = {} if not _args else _args
            
            self._delegate_write(_df.iloc[_indexes])
            #
            # @TODO: Perhaps consider writing up each segment in a thread/process (speeds things up?)
            pass

def instance(**_args):
    _proxy = lambda _agent: _agent.write(_agent.read())
    if 'source' in _args and 'target' in _args :
        
        _agent =  Transporter(**_args)  
        _proxy(_agent)
        
    else:
        _config = _args['config']
        _items =  [Transporter(**_item) for _item in _config ]
        _MAX_JOBS = 5
        _items = np.array_split(_items,_MAX_JOBS)
        for _batch in _items :
            jobs = []
            for _item in _batch :
                thread = Process(target=_proxy,args = (_item,))
                thread.start()
                jobs.append(thread)
            while jobs :
                jobs = [thread for thread in jobs if thread.is_alive()]
                time.sleep(1)
    
    pass
# class Post(Process):
#     def __init__(self,**args):
#         super().__init__()
#         self.store = args['target']
#         if 'provider' not in args['target'] :
#             pass
#             self.PROVIDER = args['target']['type']
#             # self.writer = 	transport.factory.instance(**args['target'])
#         else:
#             self.PROVIDER = args['target']['provider']
#             self.store['context'] = 'write'
#             # self.store = args['target']
#             self.store['lock'] = True
#             # self.writer = transport.instance(**args['target'])
#         #
#         # If the table doesn't exists maybe create it ?
#         #
#         self.rows = args['rows']
#         # self.rows 	=	 args['rows'].fillna('')
		
#     def log(self,**_args) :
#         if ETL.logger :
#             ETL.logger.info(**_args)
		
#     def run(self):
#         _info = {"values":self.rows} if 'couch' in self.PROVIDER else self.rows	
       
#         writer = transport.factory.instance(**self.store)
#         writer.write(_info)
#         writer.close()

		
# class ETL (Process):
#     logger = None
#     def __init__(self,**_args):
#         super().__init__()

#         self.name 	= _args['id'] if 'id' in _args else 'UNREGISTERED'
#         # if 'provider' not in _args['source'] :
#         #     #@deprecate
#         #     self.reader = transport.factory.instance(**_args['source'])
#         # else:
#         #     #
#         #     # This is the new interface
#         #     _args['source']['context'] = 'read'	
            
#         #     self.reader = transport.instance(**_args['source'])
        
#         #
#         # do we have an sql query provided or not ....
#         # self.sql = _args['source']['sql'] if 'sql' in _args['source'] else None
#         # self.cmd = _args['source']['cmd'] if 'cmd' in _args['source'] else None
#         # self._oargs = _args['target'] #transport.factory.instance(**_args['target'])
#         self._source = _args ['source']
#         self._target = _args['target']
#         self._source['context'] = 'read'
#         self._target['context'] = 'write'
       
#         self.JOB_COUNT =  _args['jobs']
#         self.jobs = []
# 		# self.logger = transport.factory.instance(**_args['logger'])
#     def log(self,**_args) :
#         if ETL.logger :
#             ETL.logger.info(**_args)
        
#     def run(self):
#         # if self.cmd :
#         #     idf = self.reader.read(**self.cmd)
#         # else:
#         #     idf = self.reader.read() 
#         # idf = pd.DataFrame(idf)		
#         # # idf = idf.replace({np.nan: None}, inplace = True)

#         # idf.columns = [str(name).replace("b'",'').replace("'","").strip() for name in idf.columns.tolist()]
#         # self.log(rows=idf.shape[0],cols=idf.shape[1],jobs=self.JOB_COUNT)

#         #
#         # writing the data to a designated data source 
#         #
#         try:
           
            
#             _log = {"name":self.name,"rows":{"input":0,"output":0}}
#             _reader = transport.factory.instance(**self._source)
#             if 'table' in self._source :
#                 _df = _reader.read()
#             else:
#                 _df = _reader.read(**self._source['cmd'])
#             _log['rows']['input'] = _df.shape[0]
#             #
#             # Let's write the input data-frame to the target ...
#             _writer  = transport.factory.instance(**self._target)
#             _writer.write(_df)
#             _log['rows']['output'] = _df.shape[0]

#             # self.log(module='write',action='partitioning',jobs=self.JOB_COUNT)
#             # rows = np.array_split(np.arange(0,idf.shape[0]),self.JOB_COUNT)
            
#             # #
#             # # @TODO: locks
#             # for i in np.arange(self.JOB_COUNT) :
#             #     # _id = ' '.join([str(i),' table ',self.name])
#             #     indexes = rows[i]
#             #     segment = idf.loc[indexes,:].copy() #.to_dict(orient='records')
#             #     _name = "partition-"+str(i)
#             #     if segment.shape[0] == 0 :
#             #         continue
                
#             #     proc = Post(target = self._oargs,rows = segment,name=_name)
#             #     self.jobs.append(proc)
#             #     proc.start()
                
#             #     self.log(module='write',action='working',segment=str(self.name),table=self.name,rows=segment.shape[0])
#             # while self.jobs :
#             # 	jobs = [job for job in proc if job.is_alive()]
#             # 	time.sleep(1)
#         except Exception as e:
#             print (e)
#         self.log(**_log)
#     def is_done(self):
#         self.jobs = [proc for proc in self.jobs if proc.is_alive()]
#         return len(self.jobs) == 0


# def instance (**_args):
#     """
#     path to configuration file
#     """
#     _path   = _args['path']
#     _config = {}
#     jobs = []
#     if os.path.exists(_path) :
#         file    =  open(_path)
#         _config = json.loads(file.read())
#         file.close()
#     if _config and type
    

# def _instance(**_args):
#     """
#     :path ,index, id
#     :param _info    list of objects with {source,target}`
#     :param logger
#     """
#     logger = _args['logger'] if 'logger' in _args else None
#     if 'path' in _args :
#         _info = json.loads((open(_args['path'])).read())
        
        
#         if 'index' in _args :
#             _index = int(_args['index'])
#             _info = _info[_index]
            
#         elif 'id' in _args :
#             _info = [_item for _item in _info if '_id' in _item and _item['id'] == _args['id']]
#             _info = _info[0] if _info else _info
#     else:
#         _info = _args['info']
    
#     if logger and type(logger) != str:
#         ETL.logger = logger 
#     elif logger == 'console':
#         ETL.logger = transport.factory.instance(provider='console',context='write',lock=True)
#     if type(_info) in [list,dict] :
#         _info = _info if type(_info) != dict else [_info]
#         #
#         # The assumption here is that the objects within the list are {source,target} 
#         jobs = []
#         for _item in _info :
            
#             _item['jobs'] = 5 if 'procs' not in _args else int(_args['procs'])
#             _job = ETL(**_item)
            
#             _job.start()
#             jobs.append(_job)
#         return jobs
        
#     else:
#         return None

# if __name__ == '__main__' :
#     _info = json.loads(open (SYS_ARGS['config']).read())
#     index = int(SYS_ARGS['index']) if 'index' in SYS_ARGS else None
#     procs = []
#     for _config in _info :
#         if 'source' in SYS_ARGS :
#             _config['source'] = {"type":"disk.DiskReader","args":{"path":SYS_ARGS['source'],"delimiter":","}}

#         _config['jobs']  = 3 if 'jobs' not in SYS_ARGS else int(SYS_ARGS['jobs'])
#         etl = ETL (**_config)
#         if index is None:	
            
#             etl.start()
#             procs.append(etl)

#         elif _info.index(_config) == index :
            
#             # print (_config)
#             procs = [etl]
#             etl.start()
#             break
#     #
#     #
#     N = len(procs)
#     while procs :
#         procs = [thread for thread in procs if not thread.is_done()]
#         if len(procs) < N :
#             print (["Finished ",(N-len(procs)), " remaining ", len(procs)])
#             N = len(procs)
#         time.sleep(1)
# 	# print ("We're done !!")