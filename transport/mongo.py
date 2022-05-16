"""
Data Transport - 1.0
Steve L. Nyemba, The Phi Technology LLC

This file is a wrapper around mongodb for reading/writing content against a mongodb server and executing views (mapreduce)
"""
from pymongo        import MongoClient
from bson.objectid  import ObjectId
from bson.binary    import Binary
import json
from datetime import datetime
import pandas as pd

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
        port = str(args['port']) if 'port' in args else '27017'
        host = args['host'] if 'host' in args else 'localhost'
        host = ":".join([host,port]) #-- Formatting host information here
        self.uid    = args['doc'] if 'doc' in args else None  #-- document identifier
        self.dbname = args['dbname'] if 'dbname' in args else args['db']
        authMechanism= 'SCRAM-SHA-256' if 'mechanism' not in args else args['mechanism']
        self._lock = False if 'lock' not in args else args['lock']

        username = password = None
        if 'username' in args and 'password' in args: 
            username = args['username']       
            password=args['password']
        if 'auth_file' in args :
            _info = json.loads((open(args['auth_file'])).read())
            username = _info['username']
            password = _info['password']
            if 'mechanism' in _info:
                authMechanism = _info['mechanism']
        
        authSource=(args['authSource'] if 'authSource' in args else self.dbname)
        
        if username and password :
            self.client = MongoClient(host,
                      username=username,
                      password=password ,
                      authSource=authSource,
                      authMechanism=authMechanism)
        else:
            self.client = MongoClient(host,maxPoolSize=10000)                    
        
        self.db = self.client[self.dbname]
        
    def isready(self):
        p = self.dbname in self.client.list_database_names() 
        q = self.uid in self.client[self.dbname].list_collection_names()
        return p and q
    def close(self):
        self.client.close()

class MongoReader(Mongo,Reader):
    """
    This class will read from a mongodb data store and return the content of a document (not a collection)
    """
    def __init__(self,**args):
        Mongo.__init__(self,**args)
    def read(self,**args):
        if 'mongo' in args :
            #
            # @TODO:
            cmd = args['mongo']
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
            collection = self.db[self.uid]                
            _filter = args['filter'] if 'filter' in args else {}
            return collection.find(_filter)
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
        collection = self.db[self.uid]
        # if type(info) == list :
        #     self.db[self.uid].insert_many(info)
        # else:
        try:
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

