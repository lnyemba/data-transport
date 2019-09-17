"""
Data Transport - 1.0
Steve L. Nyemba, The Phi Technology LLC

This file is a wrapper around mongodb for reading/writing content against a mongodb server and executing views (mapreduce)
"""
from pymongo import MongoClient
# from transport import Reader,Writer
from common import Reader, Writer
import json
class Mongo :
    """
    Basic mongodb functions are captured here
    """
    def __init__(self,**args):
        """
            :dbname     database name/identifier
            :host   host and port of the database
            :username   username for authentication
            :password   password for current user
        """
        host = args['host']
        
        if 'user' in args and 'password' in args:        
            self.client = MongoClient(host,
                      username=args['username'] ,
                      password=args['password'] ,
                      authMechanism='SCRAM-SHA-256')
        else:
            self.client = MongoClient()                    
        
        self.uid    = args['doc']  #-- document identifier
        self.dbname = args['dbname']
        self.db = self.client[self.dbname]
        
    def isready(self):
        p = self.dbname in self.client.list_database_names() 
        q = self.uid in self.client[self.dbname].list_collection_names()
        return p and q

class MongoReader(Mongo,Reader):
    """
    This class will read from a mongodb data store and return the content of a document (not a collection)
    """
    def __init__(self,**args):
        Mongo.__init__(self,**args)
    def read(self,size=-1):
        collection = self.db[self.uid]
        return collection.find({})
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
    def write(self,**args):
        # document  = self.db[self.uid].find()
        collection = self.db[self.uid]
        collection.update_one()
        self.db[self.uid].insert_one(args['row'])
    def set(self,document):
        collection = self.db[self.uid]
        if collection.count_document() > 0 :
            collection.delete({_id:self.uid})
        
        collecton.update_one({"_id":self.uid},document,True)

