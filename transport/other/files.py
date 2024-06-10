"""
This file is a wrapper around pandas built-in functionalities to handle character delimited files
"""
import pandas as pd
import numpy as np
import os
class File :	
	def __init__(self,**params):
		"""
			
			@param	path	absolute path of the file to be read
		"""
		self.path 		= params['path'] if 'path' in params else None
		self.delimiter	= params['delimiter'] if 'delimiter' in params else ','
		
	def isready(self):
		return os.path.exists(self.path) 
	def meta(self,**_args):
		return []
	
class Reader (File):
	"""
	This class is designed to read data from disk (location on hard drive)
	@pre : isready() == True
	"""
	
	def __init__(self,**_args):
		super().__init__(**_args)

	def read(self,**args):
		_path = self.path if 'path' not in args else args['path']
		_delimiter = self.delimiter if 'delimiter' not in args else args['delimiter']
		return pd.read_csv(_path,delimiter=self.delimiter)
	def stream(self,**args):
		raise Exception ("streaming needs to be implemented")
class Writer (File):

	"""
		This function writes output to disk in a designated location. The function will write a text to a text file
		- If a delimiter is provided it will use that to generate a xchar-delimited file
		- If not then the object will be dumped as is
	"""
	# THREAD_LOCK = RLock()
	def __init__(self,**_args):
		super().__init__(**_args)
		self._mode = 'w' if 'mode' not in _args else _args['mode']
	
	def write(self,info,**_args):
		"""
			This function writes a record to a designated file
			@param	label	<passed|broken|fixed|stats>
			@param	row	row to be written
		"""
		try:
			
			_delim = self.delimiter if 'delimiter' not in _args else _args['delimiter']
			_path = self.path if 'path' not  in _args else _args['path']
			_mode = self._mode if 'mode' not in _args else _args['mode']
			info.to_csv(_path,index=False,sep=_delim)
			
			pass
		except Exception as e:
			#
			# Not sure what should be done here ...
			print (e)
			pass
		finally:
			# DiskWriter.THREAD_LOCK.release()
			pass