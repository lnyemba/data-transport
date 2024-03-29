"""
Data Transport - 1.0
Steve L. Nyemba, The Phi Technology LLC

This module is designed to serve as a wrapper to a set of supported data stores :
    - couchdb
    - mongodb
    - Files (character delimited)
    - Queues (Rabbmitmq)
    - Session (Flask)
    - s3
The supported operations are read/write and providing meta data to the calling code
Requirements :
	pymongo
	boto
	couldant
@TODO:
	Enable read/writing to multiple reads/writes
"""
__author__ = 'The Phi Technology'
import numpy as np
import json
import importlib 
from multiprocessing import RLock
import queue
# import couch
# import mongo
from datetime import datetime

class IO:
	def init(self,**args):
		"""
		This function enables attributes to be changed at runtime. Only the attributes defined in the class can be changed
		Adding attributes will require sub-classing otherwise we may have an unpredictable class ...
		"""
		allowed = list(vars(self).keys())
		for field in args :
			if field not in allowed :
				continue
			value = args[field]
			setattr(self,field,value)
class IEncoder (json.JSONEncoder):
	def default (self,object):
		if type(object) == np.integer :
			return int(object)
		elif type(object) == np.floating:
			return float(object)
		elif type(object) == np.ndarray :
			return object.tolist()
		elif type(object) == datetime :
			return object.isoformat()
		else:
			return super(IEncoder,self).default(object)
				
class Reader (IO):
	"""
	This class is an abstraction of a read functionalities of a data store
	"""
	def __init__(self):
		pass
	def meta(self,**_args):
		"""
		This function is intended to return meta-data associated with what has just been read
		@return object of meta data information associated with the content of the store
		"""
		raise Exception ("meta function needs to be implemented")
	def read(self,**args):
		"""
		This function is intended to read the content of a store provided parameters to be used at the discretion of the subclass
		"""
		raise Exception ("read function needs to be implemented")		


class Writer(IO):
	def __init__(self):
		self.cache = {"default":[]}
	def log(self,**args):
		self.cache[id] = args
	def meta (self,id="default",**args):
		raise Exception ("meta function needs to be implemented")
	def format(self,row,xchar):
		if xchar is not None and isinstance(row,list):
			return xchar.join(row)+'\n'
		elif xchar is None and isinstance(row,dict):
			row = json.dumps(row)
		return row
	def write(self,**args):
		"""
		This function will write content to a store given parameters to be used at the discretion of the sub-class
		"""
		raise Exception ("write function needs to be implemented")

	def archive(self):
		"""
		It is important to be able to archive data so as to insure that growth is controlled
		Nothing in nature grows indefinitely neither should data being handled.
		"""
		raise Exception ("archive function needs to be implemented")
	def close(self):
		"""
		This function will close the persistent storage connection/handler
		"""
		pass
class ReadWriter(Reader,Writer) :
	"""
	This class implements the read/write functions aggregated
	"""
	pass
# class Console(Writer):
# 	lock = RLock()
# 	def __init__(self,**_args):
# 		self.lock = _args['lock'] if 'lock' in _args else False
# 		self.info = self.write
# 		self.debug = self.write
# 		self.log = self.write
# 		pass
# 	def write (self,logs=None,**_args):
# 		if self.lock :
# 			Console.lock.acquire()
# 		try:
# 			_params = _args if logs is None and _args else  logs
# 			if type(_params) == list:
# 				for row in _params :
# 					print (row)
# 			else:
# 				print (_params)
# 		except Exception as e :
# 			print (e)
# 		finally:
# 			if self.lock :
# 				Console.lock.release()


"""
@NOTE : Experimental !!
"""	
class Proxy :
	"""
	This class will forward a call to a function that is provided by the user code
	"""
	def __init__(self,**_args):
		self.callback = _args['callback']
	def read(self,**_args) :
		try:
			return self.callback(**_args)
		except Exception as e:
			return self.callback()

		pass
	def write(self,data,**_args):
		self.callback(data,**_args)
