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

import pandas 	as pd
import numpy 	as np
import json
import importlib 
import sys 
import sqlalchemy
if sys.version_info[0] > 2 : 
	# from transport.common import Reader, Writer,Console #, factory
	from transport import disk

	from transport import s3 as s3
	from transport import rabbitmq as queue
	from transport import couch as couch
	from transport import mongo as mongo
	from transport import sql as sql
	from transport import etl as etl
	from transport.version import __version__
	from transport import providers
else:
	from common import Reader, Writer,Console #, factory
	import disk
	import queue
	import couch
	import mongo
	import s3
	import sql
	import etl
	from version import __version__
	import providers
import psycopg2 as pg
import mysql.connector as my
from google.cloud import bigquery as bq
import nzpy as nz   #--- netezza drivers
import os

# class providers :
# 	POSTGRESQL 	= 'postgresql'
# 	MONGODB 	= 'mongodb'
	
# 	BIGQUERY	='bigquery'
# 	FILE 	= 'file'
# 	ETL = 'etl'
# 	SQLITE = 'sqlite'
# 	SQLITE3= 'sqlite'
# 	REDSHIFT = 'redshift'
# 	NETEZZA = 'netezza'
# 	MYSQL = 'mysql'
# 	RABBITMQ = 'rabbitmq'
# 	MARIADB  = 'mariadb'
# 	COUCHDB = 'couch'
# 	CONSOLE = 'console'
# 	ETL = 'etl'
# 	#
# 	# synonyms of the above
# 	BQ 		= BIGQUERY
# 	MONGO 	= MONGODB
# 	FERRETDB= MONGODB
# 	PG 		= POSTGRESQL
# 	PSQL 	= POSTGRESQL
# 	PGSQL	= POSTGRESQL
# import providers

class IEncoder (json.JSONEncoder):
	def default (self,object):
		if type(object) == np.integer :
			return int(object)
		elif type(object) == np.floating:
			return float(object)
		elif type(object) == np.ndarray :
			return object.tolist()
		else:
			return super(IEncoder,self).default(object)
class factory :
	TYPE = {"sql":{"providers":["postgresql","mysql","neteeza","bigquery","mariadb","redshift"]}}
	PROVIDERS = {
		"etl":{"class":{"read":etl.instance,"write":etl.instance}},
		# "console":{"class":{"write":Console,"read":Console}},
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
def instance(**_pargs):
	"""
	creating an instance given the provider, we should have an idea of :class, :driver
	:provider
	:read|write = {connection to the database}
	"""	
	#
	# @TODO: provide authentication file that will hold all the parameters, that will later on be used
	#
	_args = dict(_pargs,**{})
	if 'auth_file' in _args :
		path = _args['auth_file']
		file = open(path)
		_config = json.loads( file.read())
		_args = dict(_args,**_config)
		file.close()

	_provider = _args['provider']
	_context = list( set(['read','write','listen']) & set(_args.keys()) )
	if _context :
		_context = _context[0]
	else:
		_context = _args['context'] if 'context' in _args else 'read'
	# _group = None
	

	# for _id in providers.CATEGORIES :
	# 	if _provider in providers.CATEGORIES[_id] :
	# 		_group = _id
	# 		break
	# if _group :
	
	if _provider in providers.PROVIDERS and _context in providers.PROVIDERS[_provider]:
		
		# _classPointer = _getClassInstance(_group,**_args)
		_classPointer = providers.PROVIDERS[_provider][_context]
		#
		# Let us reformat the arguments
		# if 'read' in _args or 'write' in _args :
		# 	_args = _args['read'] if 'read' in _args else _args['write']
		# 	_args['provider'] = _provider
		# if _group == 'sql' :
		if _provider in providers.CATEGORIES['sql'] :
			_info = _get_alchemyEngine(**_args)

			_args = dict(_args,**_info)
			_args['driver'] = providers.DRIVERS[_provider]
			
		else:
			if _provider in providers.DEFAULT :
				_default = providers.DEFAULT[_provider]
				_defkeys = list(set(_default.keys()) - set(_args.keys()))
				if _defkeys :
					for key in _defkeys :
						_args[key] = _default[key]
			pass
		#
		# get default values from 
		
		return _classPointer(**_args)
	#
	# Let us determine the category of the provider that has been given
def _get_alchemyEngine(**_args):
	"""
	This function returns the SQLAlchemy engine associated with parameters, This is only applicable for SQL _items
	:_args	arguments passed to the factory {provider and other}
	"""
	_provider = _args['provider']
	_pargs = {}
	if _provider == providers.SQLITE3 :
		_path = _args['database'] if 'database' in _args else _args['path']
		uri = ''.join([_provider,':///',_path])
		
	else:
		
		#@TODO: Enable authentication files (private_key)
		_username = _args['username'] if 'username' in _args else ''
		_password = _args['password'] if 'password' in _args else ''
		_account = _args['account'] if 'account' in _args else ''
		_database =  _args['database']	if 'database' in _args else _args['path']
		
		if _username != '':
			_account = _username + ':'+_password+'@'
		_host = _args['host'] if 'host' in _args else ''
		_port = _args['port'] if 'port' in _args else ''
		if _provider in providers.DEFAULT :
			_default = providers.DEFAULT[_provider]
			_host = _host if _host != '' else (_default['host'] if 'host' in _default else '')
			_port = _port if _port != '' else (_default['port'] if 'port' in _default else '')
		if _port == '':		
			_port = providers.DEFAULT['port'] if 'port' in providers.DEFAULT else ''
		#

		if _host != '' and _port != '' :
			_fhost = _host+":"+str(_port) #--formatted hostname
		else:
			_fhost = _host
		# Let us update the parameters we have thus far
	#
	
	
		uri = ''.join([_provider,"://",_account,_fhost,'/',_database])
		_pargs = {'host':_host,'port':_port,'username':_username,'password':_password}
	_engine =  sqlalchemy.create_engine (uri,future=True)
	_out = {'sqlalchemy':_engine}
	
	for key in _pargs :
		if _pargs[key] != '' :
			_out[key] = _pargs[key]
	return _out
@DeprecationWarning
def _getClassInstance(_group,**_args):
	"""
	This function returns the class instance we are attempting to instanciate
	:_group		items in providers.CATEGORIES.keys()
	:_args		arguments passed to the factory class
	"""		
	# if 'read' in _args or 'write' in _args :
	# 	_context = 'read' if 'read' in _args else _args['write']
	# 	_info = _args[_context]
	# else:
	# 	_context = _args['context'] if 'context' in _args else 'read'
	# _class = providers.READ[_group] if _context == 'read' else providers.WRITE[_group]
	# if type(_class) == dict and _args['provider'] in _class:
	# 	_class = _class[_args['provider']]

	# return _class

@DeprecationWarning
def __instance(**_args):
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
			account = ''
			host = ''
			if provider not in [providers.BIGQUERY,providers.MONGODB, providers.COUCHDB, providers.SQLITE, providers.CONSOLE,providers.ETL, providers.FILE, providers.RABBITMQ] :
			# if provider not in ['bigquery','mongodb','mongo','couchdb','sqlite','console','etl','file','rabbitmq'] :
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
			elif provider in [providers.SQLITE,providers.FILE]:
				account = ''
				host = ''
				database = args['path'] if 'path' in args else args['database']

			if provider not in [providers.MONGODB, providers.COUCHDB, providers.BIGQUERY, providers.CONSOLE, providers.ETL,providers.FILE,providers.RABBITMQ] :
			# if provider not in ['mongodb','mongo','couchdb','bigquery','console','etl','file','rabbitmq'] :
				uri = ''.join([provider,"://",account,host,'/',database])
				
				e = sqlalchemy.create_engine (uri,future=True)
				args['sqlalchemy'] = e 
			
			#
			# @TODO: Include handling of bigquery with SQLAlchemy
		except Exception as e:
			print (_args)
			print (e)

		return pointer(**args) 

	return None
