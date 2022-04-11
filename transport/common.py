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
# import couch
# import mongo
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
class Console(Writer):
	lock = RLock()
	def __init__(self,**_args):
		self.lock = _args['lock'] if 'lock' in _args else False
		self.info = self.write
		self.debug = self.write
		self.log = self.write
		pass
	def write (self,info,**_args):
		if self.lock :
			Console.lock.acquire()
		try:
			if type(info) == list:
				for row in info :
					print (row)
			else:
				print (info)
		except Exception as e :
			print (e)
		finally:
			if self.lock :
				Console.lock.release()
	
# class factory :
# 	@staticmethod
# 	def instance(**args):
# 		"""
# 		This class will create an instance of a transport when providing 
# 		:type	name of the type we are trying to create
# 		:args	The arguments needed to create the instance
# 		"""
# 		source = args['type']		
# 		params = args['args']
# 		anObject = None
		
# 		if source in ['HttpRequestReader','HttpSessionWriter']:
# 			#
# 			# @TODO: Make sure objects are serializable, be smart about them !!
# 			#
# 			aClassName = ''.join([source,'(**params)'])


# 		else:
			
# 			stream = json.dumps(params)
# 			aClassName = ''.join([source,'(**',stream,')'])
# 		try:
# 			anObject = eval( aClassName)
# 			#setattr(anObject,'name',source)
# 		except Exception,e:
# 			print ['Error ',e]
# 		return anObject