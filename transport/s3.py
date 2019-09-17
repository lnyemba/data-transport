from datetime import datetime
import boto
import botocore
from smart_open import smart_open
from common import Reader, Writer
import json
from common import Reader, Writer

class s3 :
	"""
		@TODO: Implement a search function for a file given a bucket??
	"""
	def __init__(self,args) :
		"""
			This function will extract a file or set of files from s3 bucket provided
			@param access_key
			@param secret_key
			@param path		location of the file
			@param filter		filename or filtering elements
		"""
		try:
			self.s3 = boto.connect_s3(args['access_key'],args['secret_key'])
			self.bucket = self.s3.get_bucket(args['bucket'].strip(),validate=False) if 'bucket' in args else None
			# self.path = args['path']
			self.filter = args['filter'] if 'filter' in args else None
			self.filename = args['file'] if 'file' in args else None

		except Exception as e :
			self.s3 = None
			self.bucket = None
			print (e)

	def buckets(self):
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
	def __init__(self,args) :
			s3.__init__(self,args)
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
	def read(self,limit=-1) :
		if self.filename is None :
			# 
		# returning the list of files because no one file was specified.
			return self.files()
		else:
			return self.stream(10)

class s3Writer(s3,Writer) :
        def __init__(self,args) :
        	s3.__init__(self,args)
