"""
Data Transport - 1.0
Steve L. Nyemba, The Phi Technology LLC

This file is a wrapper around s3 bucket provided by AWS for reading and writing content
"""
from datetime import datetime
import boto
from boto.s3.connection import S3Connection, OrdinaryCallingFormat
import numpy as np
import botocore
from smart_open import smart_open
import sys
if sys.version_info[0] > 2 :
	from transport.common import Reader, Writer
else:
	from common import Reader, Writer
import json
from io import StringIO
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
			self.s3 = S3Connection(args['access_key'],args['secret_key'],calling_format=OrdinaryCallingFormat())			
			self.bucket = self.s3.get_bucket(args['bucket'].strip(),validate=False) if 'bucket' in args else None
			# self.path = args['path']
			self.filter = args['filter'] if 'filter' in args else None
			self.filename = args['file'] if 'file' in args else None
			self.bucket_name = args['bucket'] if 'bucket' in args else None

		except Exception as e :
			self.s3 = None
			self.bucket = None
			print (e)
	def meta(self,**args):
		"""
		:name name of the bucket
		"""
		info = self.list(**args)
		[item.open() for item in info]
		return [{"name":item.name,"size":item.size} for item in info]
	def list(self,**args):
		"""
		This function will list the content of a bucket, the bucket must be provided by the name
		:name	name of the bucket
		"""
		return list(self.s3.get_bucket(args['name']).list())


	def buckets(self):
		#
		# This function will return all buckets, not sure why but it should be used cautiously 
		# based on why the s3 infrastructure is used
		#
		return [item.name for item in self.s3.get_all_buckets()]

		# def buckets(self):
		pass
		# """
		# This function is a wrapper around the bucket list of buckets for s3
		# """
		# return self.s3.get_all_buckets()
		
        	
class s3Reader(s3,Reader) :
	"""	
		Because s3 contains buckets and files, reading becomes a tricky proposition :
		- list files		if file is None
		- stream content	if file is Not None
		@TODO: support read from all buckets, think about it
	"""
	def __init__(self,**args) :
			s3.__init__(self,**args)
	def files(self):
		r = []
		try:
			return [item.name for item in self.bucket if item.size > 0]
		except Exception as e:
			pass
		return r
	def stream(self,limit=-1):
		"""
			At this point we should stream a file from a given bucket
		"""
		key = self.bucket.get_key(self.filename.strip())
		if key is None :
			yield None
		else:
			count = 0
			with smart_open(key) as remote_file:
				for line in remote_file:
					if count == limit and limit > 0 :
						break
				yield line
				count += 1
	def read(self,**args) :
		if self.filename is None :
			# 
		# returning the list of files because no one file was specified.
			return self.files()
		else:
			limit = args['size'] if 'size' in args else -1
			return self.stream(limit)

class s3Writer(s3,Writer) :

	def __init__(self,**args) :
		s3.__init__(self,**args)
	def mkdir(self,name):
		"""
		This function will create a folder in a bucket
		:name name of the folder
		"""
		self.s3.put_object(Bucket=self.bucket_name,key=(name+'/'))
	def write(self,content):
		file = StringIO(content.decode("utf8"))
		self.s3.upload_fileobj(file,self.bucket_name,self.filename)
		pass
		
