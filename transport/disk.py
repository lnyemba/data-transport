import os
import sys


if sys.version_info[0] > 2 : 
    from transport.common import Reader, Writer #, factory
else:
	from common import Reader,Writer
# import nujson as json
import json
# from threading import Lock
import sqlite3
import pandas as pd
from multiprocessing import Lock
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
		self.path 		= params['path'] if 'path' in params else None
		self.delimiter	= params['delimiter'] if 'delimiter' in params else ','
		
	def isready(self):
		return os.path.exists(self.path) 
	def meta(self,**_args):
		return []
	def read(self,**args):
		_path = self.path if 'path' not in args else args['path']
		_delimiter = self.delimiter if 'delimiter' not in args else args['delimiter']
		return pd.read_csv(_path,delimiter=self.delimiter)
	def stream(self,**args):
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
				yield row.split(self.delimiter)
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
		super().__init__()
		self._path = params['path']
		self._delimiter = params['delimiter']
		self._mode = 'w' if 'mode' not in params else params['mode']
	# def meta(self):
	# 	return self.cache['meta']
	# def isready(self):
	# 	"""
	# 		This function determines if the class is ready for execution or not
	# 		i.e it determines if the preconditions of met prior execution
	# 	"""
	# 	return True
	# 	# p =  self.path is not None and os.path.exists(self.path)
	# 	# q = self.name is not None 
	# 	# return p and q
	# def format (self,row):
	# 	self.cache['meta']['cols'] += len(row) if isinstance(row,list) else len(row.keys())
	# 	self.cache['meta']['rows'] += 1
	# 	return (self.delimiter.join(row) if self.delimiter else json.dumps(row))+"\n"
	def write(self,info,**_args):
		"""
			This function writes a record to a designated file
			@param	label	<passed|broken|fixed|stats>
			@param	row	row to be written
		"""
		try:
			
			
			DiskWriter.THREAD_LOCK.acquire()		
			
			_delim = self._delimiter if 'delimiter' not in _args else _args['delimiter']
			_path = self._path if 'path' not  in _args else _args['path']
			_mode = self._mode if 'mode' not in _args else _args['mode']
			info.to_csv(_path,index=False,sep=_delim)
			pass
		except Exception as e:
			#
			# Not sure what should be done here ...
			pass
		finally:
			DiskWriter.THREAD_LOCK.release()
class SQLite :
	def __init__(self,**_args) :
		self.path  = _args['database'] if 'database' in _args else _args['path']
		self.conn = sqlite3.connect(self.path,isolation_level="IMMEDIATE")
		self.conn.row_factory = sqlite3.Row
		self.fields = _args['fields'] if 'fields' in _args else []
	def has (self,**_args):
		found = False
		try:
			if 'table' in _args :
				table = _args['table']
				sql = "SELECT * FROM :table limit 1".replace(":table",table) 
				_df = pd.read_sql(sql,self.conn)
				found = _df.columns.size > 0
		except Exception as e:
			pass
		return found
	def close(self):
		try:
			self.conn.close()
		except Exception as e :
			print(e)
	def apply(self,sql):
		try:
			if not sql.lower().startswith('select'):
				cursor = self.conn.cursor()
				cursor.execute(sql)
				cursor.close()
				self.conn.commit()
			else:
				return pd.read_sql(sql,self.conn)
		except Exception as e:
			print (e)
class SQLiteReader (SQLite,DiskReader):
	def __init__(self,**args):
		super().__init__(**args)
		# DiskReader.__init__(self,**args)
		# self.path  = args['database'] if 'database' in args else args['path']
		# self.conn = sqlite3.connect(self.path,isolation_level=None)
		# self.conn.row_factory = sqlite3.Row
		self.table = args['table'] if 'table' in args else None
	def read(self,**args):
		if 'sql' in args :
			sql = args['sql']			
		elif 'filter' in args :
			sql = "SELECT :fields FROM ",self.table, "WHERE (:filter)".replace(":filter",args['filter'])
			sql = sql.replace(":fields",args['fields']) if 'fields' in args else sql.replace(":fields","*")
		else:
			sql = ' '.join(['SELECT * FROM ',self.table])
		if 'limit' in args :
			sql = sql + " LIMIT "+args['limit']
		return  pd.read_sql(sql,self.conn)
	def close(self):
		try:
			self.conn.close()
		except Exception as e :
			pass

class SQLiteWriter(SQLite,DiskWriter) :
	connection = None
	LOCK = Lock()
	def __init__(self,**args):
		"""
		:path
		:fields json|csv
		"""
		# DiskWriter.__init__(self,**args)
		super().__init__(**args)
		self.table = args['table'] if 'table' in args else None
		
		# self.conn = sqlite3.connect(self.path,isolation_level="IMMEDIATE")
		# self.conn.row_factory = sqlite3.Row
		# self.fields = args['fields'] if 'fields' in args else []
		
		if self.fields and not self.isready() and self.table:
			self.init(self.fields)
		SQLiteWriter.connection = self.conn	
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
			
			return r[0][0] != 0
		except Exception as e:
			pass
		return 0
		#
		# If the table doesn't exist we should create it
		#
	def write(self,info,**_args):
		"""
		"""
		
		if not self.fields :
			if type(info) == pd.DataFrame :
				_columns = list(info.columns) 
			self.init(list(info.keys()))
		
		if type(info) == dict :
			info = [info]
		elif type(info) == pd.DataFrame :
			info = info.fillna('')
			info = info.to_dict(orient='records')
		
		SQLiteWriter.LOCK.acquire()
		try:
			
			cursor = self.conn.cursor()	
			sql = " " .join(["INSERT INTO ",self.table,"(", ",".join(self.fields) ,")", "values(:values)"])
			for row in info :
				stream =["".join(["",value,""]) if type(value) == str else value for value in row.values()]
				stream = json.dumps(stream).replace("[","").replace("]","")
				
				
				self.conn.execute(sql.replace(":values",stream) )
				# cursor.commit()
			
			self.conn.commit()
				# print (sql)
		except Exception as e :
			print (e)
			pass
		SQLiteWriter.LOCK.release()