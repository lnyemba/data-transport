import os
import sys
if sys.version_info[0] > 2 : 
    from transport.common import Reader, Writer #, factory
else:
	from common import Reader,Writer
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
		self.path 		= params['path'] ;
		self.delimiter	= params['delimiter'] if 'delimiter' in params else None
	def isready(self):
		return os.path.exists(self.path) 
	def read(self,**args):
		"""
		This function reads the rows from a designated location on disk
		@param	size	number of rows to be read, -1 suggests all rows
		"""
		size = -1 if 'size' not in args else int(args['size'])
		f = open(self.path,'rU') 
		i = 1
		for row in f:
			
			i += 1
			if size == i:
				break
			if self.delimiter :
				yield row.split(self.char)
			yield row
		f.close()
class DiskWriter(Writer):
	"""
		This function writes output to disk in a designated location. The function will write a text to a text file
		- If a delimiter is provided it will use that to generate a xchar-delimited file
		- If not then the object will be dumped as is
	"""

	def __init__(self,**params):
		Writer.__init__(self)
		self.cache['meta'] = {'cols':0,'rows':0,'delimiter':None}
		if 'path' in params:
			self.path = params['path']
		else:
			self.path = 'data-transport.log'
		self.delimiter = params['delimiter'] if 'delimiter' in params else None
		# if 'name' in params:
		# 	self.name = params['name'];
		# else:
		# 	self.name = 'data-transport.log'
		# if os.path.exists(self.path) == False:
		# 	os.mkdir(self.path)
	def meta(self):
		return self.cache['meta']
	def isready(self):
		"""
			This function determines if the class is ready for execution or not
			i.e it determines if the preconditions of met prior execution
		"""
		return True
		# p =  self.path is not None and os.path.exists(self.path)
		# q = self.name is not None 
		# return p and q
	def format (self,row):
		self.cache['meta']['cols'] += len(row) if isinstance(row,list) else len(row.keys())
		self.cache['meta']['rows'] += 1
		return (self.delimiter.join(row) if self.delimiter else json.dumps(row))+"\n"
	def write(self,info):
		"""
			This function writes a record to a designated file
			@param	label	<passed|broken|fixed|stats>
			@param	row	row to be written
		"""
		f = open(self.path,'a')
		f.write(self.format(info))
		f.close()

		
