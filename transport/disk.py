import os
import sys
if sys.version_info[0] > 2 : 
    from transport.common import Reader, Writer #, factory
else:
	from common import Reader,Writer
import json
from threading import Lock
import sqlite3

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
	THREAD_LOCK = Lock()
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
		try:
			DiskWriter.THREAD_LOCK.acquire()
			f = open(self.path,'a')
			if self.delimiter :
				if type(info) == list :
					for row in info :
						f.write(self.format(row))
				else:
					f.write(self.format(info))
			else:
				if not type(info) == str :
					f.write(json.dumps(info))
				else:
					f.write(info)
			f.close()
		except Exception as e:
			#
			# Not sure what should be done here ...
			pass
		finally:
			DiskWriter.THREAD_LOCK.release()

class SQLiteWriter(DiskWriter) :
	def __init__(self,**args):
		"""
		:path
		:fields json|csv
		"""
		DiskWriter.__init__(self,**args)
		self.table = args['table']
		
		self.conn = sqlite3.connect(self.path,isolation_level=None)
		self.conn.row_factory = sqlite3.Row
		self.fields = args['fields'] if 'fields' in args else []
		
		if self.fields and not self.isready():
			self.init(self.fields)
			
	def init(self,fields):
		self.fields = fields;
		sql = " ".join(["CREATE TABLE  IF NOT EXISTS ",self.table," (", ",".join(self.fields),")"])
		
		cursor = self.conn.cursor()
		cursor.execute(sql)
		cursor.close()
		self.conn.commit()
	def isready(self):
		try:
			sql = "SELECT count(*) FROM sqlite_master where name=':table'"
			sql = sql.replace(":table",self.table)
			cursor = self.conn.cursor()

			r = cursor.execute(sql)
			r = r.fetchall()
			cursor.close()
			
			return r[0][0]
		except Exception as e:
			pass
		return 0
		#
		# If the table doesn't exist we should create it
		#
	def write(self,info):
		"""
		"""
		
		if not self.fields :
			self.init(list(info.keys()))
		
		if type(info) != list :
			info = [info]
		cursor = self.conn.cursor()
		
		
		sql = " " .join(["INSERT INTO ",self.table,"(", ",".join(self.fields) ,")", "values(':values')"])
		for row in info :
			stream = json.dumps(row)
			stream = stream.replace("'","''")
			cursor.execute(sql.replace(":values",stream) )
			# self.conn.commit()
				# print (sql)
			
