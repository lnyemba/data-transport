from flask import request, session
from datetime import datetime
import re
# from transport.common import Reader, Writer
import json
import requests
from io import StringIO
import pandas as pd

def template():
	return {'url':None, 'headers':{'key':'value'}}
class Reader:
	"""
	This class is designed to read data from an Http request file handler provided to us by flask
	The file will be heald in memory and processed accordingly
	NOTE: This is inefficient and can crash a micro-instance (becareful)
	"""

	def __init__(self,**_args):
		self._url = _args['url']
		self._headers = None if 'headers' not in _args else _args['headers']
					
	# def isready(self):
	# 	return self.file_length > 0
	def format(self,_response):
		_mimetype= _response.headers['Content-Type']
		if _mimetype == 'text/csv' or 'text/csv':
			_content = _response.text
			return pd.read_csv(StringIO(_content))
		#
		# @TODO: Add support for excel, JSON and other file formats that fit into a data-frame
		#
		
		return _response.text
	def read(self,**_args):
		if self._headers :
			r = requests.get(self._url,headers = self._headers)
		else:
			r = requests.get(self._url,headers = self._headers)
		return self.format(r)
		
class Writer:
	"""
	This class is designed to submit data to an endpoint (url)
	"""
	def __init__(self,**_args):
		"""
			@param key	required session key
		"""
		self._url = _args['url']
		self._name = _args['name']
		self._method = 'post' if 'method' not in _args else _args['method']
		
		# self.session = params['queue']
		# self.session['sql'] = []
		# self.session['csv'] = []
		# self.tablename = re.sub('..+$','',params['filename'])
		# self.session['uid'] = params['uid']
		#self.xchar = params['xchar']
			
		
	def format_sql(self,row):
		values = "','".join([col.replace('"','').replace("'",'') for col in row])
		return "".join(["INSERT INTO :table VALUES('",values,"');\n"]).replace(':table',self.tablename)		
	def isready(self):
		return True
	def write(self,_data,**_args):
		#
		#
		_method = self._method if 'method' not in _args else _args['method']
		_method = _method.lower()
		_mimetype = 'text/csv'
		if type(_data) == dict :
			_mimetype = 'application/json'
			_content = _data
		else:
			_content = _data.to_dict(orient='records')
		_headers = {'Content-Type':_mimetype}
		_pointer = getattr(requests,_method)
		
		_pointer ({self._name:_content},headers=_headers)


		# label = params['label']
		# row = params ['row']
		
		# if label == 'usable':
		# 	self.session['csv'].append(self.format(row,','))
		# 	self.session['sql'].append(self.format_sql(row))
