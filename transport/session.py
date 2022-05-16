from flask import request, session
from datetime import datetime
import re
from common import Reader, Writer
import json

class HttpRequestReader(Reader):
	"""
	This class is designed to read data from an Http request file handler provided to us by flask
	The file will be heald in memory and processed accordingly
	NOTE: This is inefficient and can crash a micro-instance (becareful)
	"""

	def __init__(self,**params):
		self.file_length = 0
		try:
			
			#self.file = params['file']	
			#self.file.seek(0, os.SEEK_END)
			#self.file_length = self.file.tell()
			
			#print 'size of file ',self.file_length
			self.content = params['file'].readlines()
			self.file_length = len(self.content)
		except Exception as e:
			print ("Error ... ",e)
			pass
		
	def isready(self):
		return self.file_length > 0
	def read(self,size =-1):
		i = 1
		for row in self.content:
			i += 1
			if size == i:
				break
			yield row
		
class HttpSessionWriter(Writer):
	"""
		This class is designed to write data to a session/cookie
	"""
	def __init__(self,**params):
		"""
			@param key	required session key
		"""
		self.session = params['queue']
		self.session['sql'] = []
		self.session['csv'] = []
		self.tablename = re.sub('..+$','',params['filename'])
		self.session['uid'] = params['uid']
		#self.xchar = params['xchar']
			
		
	def format_sql(self,row):
		values = "','".join([col.replace('"','').replace("'",'') for col in row])
		return "".join(["INSERT INTO :table VALUES('",values,"');\n"]).replace(':table',self.tablename)		
	def isready(self):
		return True
	def write(self,**params):
		label = params['label']
		row = params ['row']
		
		if label == 'usable':
			self.session['csv'].append(self.format(row,','))
			self.session['sql'].append(self.format_sql(row))
