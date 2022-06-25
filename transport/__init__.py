"""
Data Transport, The Phi Technology LLC
Steve L. Nyemba, steve@the-phi.com

This library is designed to serve as a wrapper to a set of supported data stores :
    - couchdb
    - mongodb
    - Files (character delimited)
    - Queues (RabbmitMq)
    - Session (Flask)
    - s3
	- sqlite
The supported operations are read/write and providing meta data to the calling code
Requirements :
	pymongo
	boto
	couldant
The configuration for the data-store is as follows :
	e.g:
	mongodb
		provider:'mongodb',[port:27017],[host:localhost],db:<name>,doc:<_name>,context:<read|write>
"""
__author__ = 'The Phi Technology'
import pandas 	as pd
import numpy 	as np
import json
import importlib 
import sys 
import sqlalchemy
if sys.version_info[0] > 2 : 
	from transport.common import Reader, Writer,Console #, factory
	from transport import disk

	from transport import s3 as s3
	from transport import rabbitmq as queue
	from transport import couch as couch
	from transport import mongo as mongo
	from transport import sql as sql
	from transport import etl as etl
else:
	from common import Reader, Writer,Console #, factory
	import disk
	import queue
	import couch
	import mongo
	import s3
	import sql
	import etl
import psycopg2 as pg
import mysql.connector as my
from google.cloud import bigquery as bq
import nzpy as nz   #--- netezza drivers
import os



class factory :
	TYPE = {"sql":{"providers":["postgresql","mysql","neteeza","bigquery","mariadb","redshift"]}}
	PROVIDERS = {
		"etl":{"class":{"read":etl.instance,"write":etl.instance}},
		"console":{"class":{"write":Console,"read":Console}},
		"file":{"class":{"read":disk.DiskReader,"write":disk.DiskWriter}},
		"sqlite":{"class":{"read":disk.SQLiteReader,"write":disk.SQLiteWriter}},
        "postgresql":{"port":5432,"host":"localhost","database":None,"driver":pg,"default":{"type":"VARCHAR"},"class":{"read":sql.SQLReader,"write":sql.SQLWriter}},
        "redshift":{"port":5432,"host":"localhost","database":None,"driver":pg,"default":{"type":"VARCHAR"},"class":{"read":sql.SQLReader,"write":sql.SQLWriter}},
        "bigquery":{"class":{"read":sql.BQReader,"write":sql.BQWriter}},
        "mysql":{"port":3306,"host":"localhost","default":{"type":"VARCHAR(256)"},"driver":my,"class":{"read":sql.SQLReader,"write":sql.SQLWriter}},
        "mariadb":{"port":3306,"host":"localhost","default":{"type":"VARCHAR(256)"},"driver":my,"class":{"read":sql.SQLReader,"write":sql.SQLWriter}},
		"mongo":{"port":27017,"host":"localhost","class":{"read":mongo.MongoReader,"write":mongo.MongoWriter}},		
		"couch":{"port":5984,"host":"localhost","class":{"read":couch.CouchReader,"write":couch.CouchWriter}},		
        "netezza":{"port":5480,"driver":nz,"default":{"type":"VARCHAR(256)"},"class":{"read":sql.SQLReader,"write":sql.SQLWriter}},
		"rabbitmq":{"port":5672,"host":"localhost","class":{"read":queue.QueueReader,"write":queue.QueueWriter,"listen":queue.QueueListener,"listener":queue.QueueListener},"default":{"type":"application/json"}}}
	#
	# creating synonyms
	PROVIDERS['mongodb'] = PROVIDERS['mongo']
	PROVIDERS['couchdb'] = PROVIDERS['couch']
	PROVIDERS['bq'] 	 = PROVIDERS['bigquery']
	PROVIDERS['sqlite3'] = PROVIDERS['sqlite']
	PROVIDERS['rabbit'] = PROVIDERS['rabbitmq']
	PROVIDERS['rabbitmq-server'] = PROVIDERS['rabbitmq']
	
	@staticmethod
	def instance(**_args):
		if 'type' in _args :
			#
			# Legacy code being returned
			return factory._instance(**_args);
		else:
			return instance(**_args)
	@staticmethod
	def _instance(**args):
		"""
		This class will create an instance of a transport when providing 
		:type	name of the type we are trying to create
		:args	The arguments needed to create the instance
		"""
		source = args['type']		
		params = args['args']
		anObject = None
		
		if source in ['HttpRequestReader','HttpSessionWriter']:
			#
			# @TODO: Make sure objects are serializable, be smart about them !!
			#
			aClassName = ''.join([source,'(**params)'])
			

		else:
			
			stream = json.dumps(params)
			aClassName = ''.join([source,'(**',stream,')'])
			
		try:
			anObject = eval( aClassName)
			#setattr(anObject,'name',source)
		except Exception as e:
			print(['Error ',e])
		return anObject

import time
def instance(**_args):
	"""

	@param provider	{file,sqlite,postgresql,redshift,bigquery,netezza,mongo,couch ...}
	@param context	read|write|rw
	@param _args	argument to got with the datastore (username,password,host,port ...)
	"""
	
	provider = _args['provider']
	context = _args['context']if 'context' in _args else None
	_id = context if context in list(factory.PROVIDERS[provider]['class'].keys()) else 'read'
	if _id :
		args = {'provider':_id}
		for key in factory.PROVIDERS[provider] :
			if key == 'class' :
				continue 
			value = factory.PROVIDERS[provider][key]
			args[key] = value
		#
		#
		
		args = dict(args,**_args)
		
		# print (provider in factory.PROVIDERS)
		if 'class' in factory.PROVIDERS[provider]:
			pointer = factory.PROVIDERS[provider]['class'][_id] 
		else:
			pointer = sql.SQLReader if _id == 'read' else sql.SQLWriter
		#
		# Let us try to establish an sqlalchemy wrapper
		try:
			
			host = ''
			if provider not in ['bigquery','mongodb','mongo','couchdb','sqlite','console','etl','file','rabbitmq'] :
				#
				# In these cases we are assuming RDBMS and thus would exclude NoSQL and BigQuery
				username = args['username'] if 'username' in args else ''
				password = args['password'] if 'password' in args else ''
				if username == '' :
					account = ''
				else:
					account = username + ':'+password+'@'
				host = args['host'] 
				if 'port' in args :
					host = host+":"+str(args['port'])
				
				database =  args['database']	
			elif provider == 'sqlite':
				account = ''
				host = ''
				database = args['path'] if 'path' in args else args['database']
			if provider not in ['mongodb','mongo','couchdb','bigquery','console','etl','file','rabbitmq'] :
				uri = ''.join([provider,"://",account,host,'/',database])
				
				e = sqlalchemy.create_engine (uri,future=True)
				args['sqlalchemy'] = e 
			
			#
			# @TODO: Include handling of bigquery with SQLAlchemy
		except Exception as e:
			print (e)

		return pointer(**args) 

	return None
