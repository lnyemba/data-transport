"""
Data Transport - 1.0
Steve L. Nyemba, The Phi Technology LLC

This file is a wrapper around s3 bucket provided by AWS for reading and writing content
"""
from datetime import datetime
import boto3
# from boto.s3.connection import S3Connection, OrdinaryCallingFormat
import numpy as np
import botocore
from smart_open import smart_open
import sys

import json
from io import StringIO
import pandas as pd
import json

class s3 :
	"""
		@TODO: Implement a search function for a file given a bucket??
	"""
	def __init__(self,**args) :
		"""
			This function will extract a file or set of files from s3 bucket provided
			@param access_key
			@param secret_key
			@param path		location of the file
			@param filter		filename or filtering elements
		"""
		try:
			self._client = boto3.client('s3',aws_access_key_id=args['access_key'],aws_secret_access_key=args['secret_key'],region_name=args['region'])
			self._bucket_name = args['bucket']	
			self._file_name = args['file']
			self._region = args['region']
		except Exception as e :
			print (e)
			pass
	def has(self,**_args):
		_found = None
		try:
			if 'file' in _args and 'bucket' in _args:
				_found = self.meta(**_args)
			elif 'bucket' in _args and not 'file' in _args:
				_found =  self._client.list_objects(Bucket=_args['bucket']) 
			elif 'file' in _args and not 'bucket' in _args :
				_found = self.meta(bucket=self._bucket_name,file = _args['file'])
		except Exception as e:
			_found = None
			pass
		return type(_found) == dict
	def meta(self,**args):
		"""
		This function will return information either about the file in a given bucket
		:name name of the bucket
		"""
		_bucket = self._bucket_name if 'bucket' not in args else args['bucket']
		_file =  self._file_name if 'file' not in args else args['file']
		_data = self._client.get_object(Bucket=_bucket,Key=_file)
		return _data['ResponseMetadata']
	def close(self):
		self._client.close()	
        	
class Reader(s3) :
	"""	
		Because s3 contains buckets and files, reading becomes a tricky proposition :
		- list files		if file is None
		- stream content	if file is Not None
		@TODO: support read from all buckets, think about it
	"""
	def __init__(self,**_args) :
			super().__init__(**_args)
	
	def _stream(self,**_args):
		"""
			At this point we should stream a file from a given bucket
		"""
		_object = self._client.get_object(Bucket=_args['bucket'],Key=_args['file'])
		_stream = None
		try:
			_stream = _object['Body'].read()
		except Exception as e:
			pass
		if not _stream :
			return None
		if _object['ContentType'] in ['text/csv'] :
			return pd.read_csv(StringIO(str(_stream).replace("\\n","\n").replace("\\r","").replace("\'","")))
		else:
			return _stream
		
	def read(self,**args) :
		
		_name = self._file_name if 'file' not in args else args['file']
		_bucket = args['bucket'] if 'bucket' in args else self._bucket_name
		return self._stream(bucket=_bucket,file=_name)
		

class Writer(s3) :
	"""
	
	"""
	def __init__(self,**_args) :
		super().__init__(**_args)
		#
		# 
		if not self.has(bucket=self._bucket_name) :
			self.make_bucket(self._bucket_name)
	def make_bucket(self,bucket_name):
		"""
		This function will create a folder in a bucket,It is best that the bucket is organized as a namespace
		:name name of the folder
		"""
		
		self._client.create_bucket(Bucket=bucket_name,CreateBucketConfiguration={'LocationConstraint': self._region})
	def write(self,_data,**_args):
		"""
		This function will write the data to the s3 bucket, files can be either csv, or json formatted files
		"""
		if type(_data) == pd.DataFrame :
			_stream = _data.to_csv(index=False)
		elif type(_data) == dict :
			_stream = json.dumps(_data)
		else:
			_stream = _data
		file = StringIO(_stream)
		bucket = self._bucket_name if 'bucket' not in _args else _args['bucket']
		file_name = self._file_name if 'file' not in _args else _args['file']
		self._client.put_object(Bucket=bucket, Key = file_name, Body=_stream)
		pass
		
