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
import gridfs
# from transport import Reader,Writer
import sys
if sys.version_info[0] > 2 :
	from transport.common import Reader, Writer
else:
	from common import Reader, Writer
import json
import re
class Mongo :
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
        host = args['host'] if 'host' in args else 'localhost:27017'
        
        if 'user' in args and 'password' in args:        
            self.client = MongoClient(host,
                      username=args['username'] ,
                      password=args['password'] ,
                      authMechanism='SCRAM-SHA-256')
        else:
            self.client = MongoClient(host)                    
        
        self.uid    = args['doc']  #-- document identifier
        self.dbname = args['dbname'] if 'dbname' in args else args['db']
        self.db = self.client[self.dbname]
        
    def isready(self):
        p = self.dbname in self.client.list_database_names() 
        q = self.uid in self.client[self.dbname].list_collection_names()
        return p and q
    def close(self):
        self.db.close()

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
                if 'cursor' in out :
                    key = 'firstBatch' if 'firstBatch' in out['cursor'] else 'nextBatch'
                else:
                    key = 'n'
                if 'cursor' in out and out['cursor'][key] :
                    r += list(out['cursor'][key])
                elif out[key]:
                    r.append (out[key]) 
                    # yield out['cursor'][key]
                if key not in ['firstBatch','nextBatch'] or ('cursor' in out and out['cursor']['id']  == 0) :
                    break
                else:
                    out = self.db.command({"getMore":out['cursor']['id'],"collection":out['cursor']['ns'].split(".")[-1]}) 
                
                
            return r
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
    def write(self,info):
        """
        This function will write to a given collection i.e add a record to a collection (no updates)
        @param info new record in the collection to be added
        """
        # document  = self.db[self.uid].find()
        collection = self.db[self.uid]
        # if type(info) == list :
        #     self.db[self.uid].insert_many(info)
        # else:
        if (type(info) == list) :
            self.db[self.uid].insert_many(info)
        else:
            self.db[self.uid].insert_one(info)
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
        # collecton.update_one({"_id":self.uid},document,True)

