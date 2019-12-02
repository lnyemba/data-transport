import os
from .__init__ import Reader,Writer
import json

class DiskReader(Reader) :
	"""
	This class is designed to read data from disk (location on hard drive)
	@pre : isready() == True
	"""

	def __init__(self,**params):
		"""
			@param	path	absolute path of the file to be read
		"""

		Reader.__init__(self)
		self.path = params['path'] ;

	def isready(self):
		return os.path.exists(self.path) 
	def read(self,size=-1):
		"""
		This function reads the rows from a designated location on disk
		@param	size	number of rows to be read, -1 suggests all rows
		"""

		f = open(self.path,'rU') 
		i = 1
		for row in f:
			
			i += 1
			if size == i:
				break
			yield row
		f.close()
class DiskWriter(Writer):
	"""
		This function writes output to disk in a designated location
	"""

	def __init__(self,**params):
		if 'path' in params:
			self.path = params['path']
		else:
			self.path = None
		if 'name' in params:
			self.name = params['name'];
		else:
			self.name = 'out.log'
		# if os.path.exists(self.path) == False:
		# 	os.mkdir(self.path)

	def isready(self):
		"""
			This function determines if the class is ready for execution or not
			i.e it determines if the preconditions of met prior execution
		"""
		
		p =  self.path is not None and os.path.exists(self.path)
		q = self.name is not None 
		return p and q
	def write(self,**params):
		"""
			This function writes a record to a designated file
			@param	label	<passed|broken|fixed|stats>
			@param	row	row to be written
		"""

		# label 	= params['label']
		row 	= params['row']
		# xchar = None
		# if 'xchar' is not None:
		# 	xchar 	= params['xchar']
		#path = ''.join([self.path,os.sep,label])
		# path = ''.join([self.path,os.sep,self.name])
		#if os.path.exists(path) == False:
		#	os.mkdir(path) ;
		# path = ''.join([path,os.sep,self.name]) 
		f = open(self.path,'a')
		if isinstance(row,object):
			row = json.dumps(row)
		#row = self.format(row,xchar);
		f.write(row+"\n")
		f.close()
		
