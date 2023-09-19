"""
Data Transport - 1.0
Steve L. Nyemba, The Phi Technology LLC

This file is a wrapper around mongodb for reading/writing content against a mongodb server and executing views (mapreduce)
"""
from pymongo        import MongoClient
from bson.objectid  import ObjectId
from bson.binary    import Binary
# import nujson as json
from datetime import datetime
import pandas as pd
import numpy as np
import gridfs
# from transport import Reader,Writer
import sys
if sys.version_info[0] > 2 :
	from transport.common import Reader, Writer
else:
	from common import Reader, Writer
import json
import re
from multiprocessing import Lock, RLock
class Mongo :
    lock = RLock()
    """
    Basic mongodb functions are captured here
    """
    def __init__(self,**args):
        """
            :dbname     database name/identifier
            :host       host and port of the database by default localhost:27017
            :username   username for authentication
            :password   password for current user
        """

        self.mechanism= 'SCRAM-SHA-256' if 'mechanism' not in args else args['mechanism']
        # authSource=(args['authSource'] if 'authSource' in args else self.dbname)
        self._lock = False if 'lock' not in args else args['lock']
        self.dbname = None
        username = password = None
        if 'auth_file' in args :
            _info = json.loads((open(args['auth_file'])).read())

            
        else:
            _info = {}
        _args = dict(args,**_info)
        _map = {'dbname':'db','database':'db','table':'uid','collection':'uid','col':'uid','doc':'uid'}
        for key in _args :
            if key in ['username','password'] :
                username = _args['username'] if key=='username' else username
                password = _args['password'] if key == 'password' else password
                continue
            value = _args[key]
            if key in _map :
                key = _map[key]
            
            self.setattr(key,value)
        #
        # Let us perform aliasing in order to remain backwards compatible
        
        self.dbname = self.db if hasattr(self,'db')else self.dbname
        self.uid    = _args['table'] if 'table' in _args else (_args['doc'] if 'doc' in _args else (_args['collection'] if 'collection' in _args else None))
        if username and password :
            self.client = MongoClient(self.host,
                      username=username,
                      password=password ,
                      authSource=self.authSource,
                      authMechanism=self.mechanism)
            
        else:
            self.client = MongoClient(self.host,maxPoolSize=10000)                    
        
        self.db = self.client[self.dbname]
        
    def isready(self):
        p = self.dbname in self.client.list_database_names() 
        q = self.uid in self.client[self.dbname].list_collection_names()
        return p and q
    def setattr(self,key,value):
        _allowed = ['host','port','db','doc','authSource','mechanism']
        if key in _allowed :
            setattr(self,key,value)
        pass
    def close(self):
        self.client.close()
    def meta(self,**_args):
        return []
class MongoReader(Mongo,Reader):
    """
    This class will read from a mongodb data store and return the content of a document (not a collection)
    """
    def __init__(self,**args):
        Mongo.__init__(self,**args)
    def read(self,**args):
        
        if 'mongo' in args or 'cmd' in args or 'pipeline' in args:
            #
            # @TODO:
            cmd = {}
            if 'pipeline' in args :
                cmd['pipeline']= args['pipeline']
            if 'aggregate' not in cmd :
                cmd['aggregate'] = self.collection
            if 'pipeline' not in args or 'aggregate' not in cmd :
                cmd = args['mongo'] if 'mongo' in args else args['cmd']
            if "aggregate" in cmd :
                if "allowDiskUse" not in cmd :
                    cmd["allowDiskUse"] = True
                if "cursor" not in cmd :
                    cmd["cursor"] = {}
            r =  []
            out = self.db.command(cmd)
            #@TODO: consider using a yield (generator) works wonders
            while True :
                if 'values' in out :
                    r += out['values']
                if 'cursor' in out :
                    key = 'firstBatch' if 'firstBatch' in out['cursor'] else 'nextBatch'
                else:
                    key = 'n'
                if 'cursor' in out and out['cursor'][key] :
                    r += list(out['cursor'][key])
                elif key in out and out[key]:
                    r.append (out[key]) 
                    # yield out['cursor'][key]
                if key not in ['firstBatch','nextBatch'] or ('cursor' in out and out['cursor']['id']  == 0) :
                    break
                else:
                    out = self.db.command({"getMore":out['cursor']['id'],"collection":out['cursor']['ns'].split(".")[-1]}) 
                
                
            return pd.DataFrame(r)
        else:
            
            
            if 'table' in args  or 'collection' in args :
                if 'table' in args:
                    _uid = args['table']
                elif 'collection' in args :
                    _uid = args['collection']
                else:
                    _uid = self.uid 
            else:
                _uid = self.uid
            collection = self.db[_uid]                
            _filter = args['filter'] if 'filter' in args else {}
            _df =  pd.DataFrame(collection.find(_filter))
            columns = _df.columns.tolist()[1:]
            return _df[columns]
    def view(self,**args):
        """
        This function is designed to execute a view (map/reduce) operation
        """
        pass
class MongoWriter(Mongo,Writer):
    """
    This class is designed to write to a mongodb collection within a database
    """
    def __init__(self,**args):
        Mongo.__init__(self,**args)
    def upload(self,**args) :
        """
        This function will upload a file to the current database (using GridFS)
        :param  data        binary stream/text to be stored
        :param  filename    filename to be used
        :param  encoding    content_encoding (default utf-8)
        
        """
        if 'encoding' not in args :
            args['encoding'] = 'utf-8'
        gfs = GridFS(self.db)
        gfs.put(**args)

    def archive(self):
        """
        This function will archive documents to the 
        """
        collection = self.db[self.uid]
        rows  = list(collection.find())
        for row in rows :
            if type(row['_id']) == ObjectId :
                row['_id'] = str(row['_id'])
        stream = Binary(json.dumps(collection).encode())
        collection.delete_many({})
        now = "-".join([str(datetime.now().year()),str(datetime.now().month), str(datetime.now().day)])
        name = ".".join([self.uid,'archive',now])+".json"
        description = " ".join([self.uid,'archive',str(len(rows))])
        self.upload(filename=name,data=stream,description=description,content_type='application/json')
        # gfs = GridFS(self.db)
        # gfs.put(filename=name,description=description,data=stream,encoding='utf-8')
        # self.write({{"filename":name,"file":stream,"description":descriptions}})
        
            
        pass
    def write(self,info,**_args):
        """
        This function will write to a given collection i.e add a record to a collection (no updates)
        @param info new record in the collection to be added
        """
        # document  = self.db[self.uid].find()
        #collection = self.db[self.uid]
        # if type(info) == list :
        #     self.db[self.uid].insert_many(info)
        # else:
        try:
            if 'table' in _args or 'collection' in _args :
                _uid = _args['table'] if 'table' in _args else _args['collection']
            else:
                _uid = self.uid if 'doc' not in _args else _args['doc']
            if self._lock :
                Mongo.lock.acquire()
            if type(info) == list or type(info) == pd.DataFrame :
                self.db[_uid].insert_many(info if type(info) == list else info.to_dict(orient='records'))
            else:
                self.db[_uid].insert_one(info)
        finally:
            if self._lock :
                Mongo.lock.release()
    def set(self,document):
        """
        if no identifier is provided the function will delete the entire collection and set the new document.
        Please use this function with great care (archive the content first before using it... for safety)
        """

        collection = self.db[self.uid]
        if collection.count_document() > 0  and '_id' in document:
            id = document['_id']
            del document['_id']
            collection.find_one_and_replace({'_id':id},document)
        else:
            collection.delete_many({})
            self.write(info)
    def close(self):
        Mongo.close(self)
        # collecton.update_one({"_id":self.uid},document,True)

