"""
	This file implements data transport stuctures in order to allow data to be moved to and from anywhere
	We can thus read data from disk and write to the cloud,queue, or couchdb or SQL
"""
from flask import request, session
import os
import pika
import json
import numpy as np
from couchdbkit import Server
import re
from csv import reader
from datetime import datetime
import boto
import botocore
from smart_open import smart_open
"""
	@TODO: Write a process by which the class automatically handles reading and creating a preliminary sample and discovers the meta data
"""
class Reader:
	def __init__(self):
		self.nrows = 0
		self.xchar = None
		
	def row_count(self):		
		content = self.read()
		return np.sum([1 for row in content])
	"""
		This function determines the most common delimiter from a subset of possible delimiters. It uses a statistical approach to guage the distribution of columns for a given delimiter
	"""
	def delimiter(self,sample):
		
		m = {',':[],'\t':[],'|':[],'\x3A':[]} 
		delim = m.keys()
		for row in sample:
			for xchar in delim:
				if row.split(xchar) > 1:	
					m[xchar].append(len(row.split(xchar)))
				else:
					m[xchar].append(0)
				
				
					
		#
		# The delimiter with the smallest variance, provided the mean is greater than 1
		# This would be troublesome if there many broken records sampled
		#
		m = {id: np.var(m[id]) for id in m.keys() if m[id] != [] and int(np.mean(m[id]))>1}
		index = m.values().index( min(m.values()))
		xchar = m.keys()[index]
		
		return xchar
	"""
		This function determines the number of columns of a given sample
		@pre self.xchar is not None
	"""
	def col_count(self,sample):
		
		m = {}
		i = 0
		
		for row in sample:
			row = self.format(row)
			id = str(len(row))
			#id = str(len(row.split(self.xchar))) 
			
			if id not in m:
				m[id] = 0
			m[id] = m[id] + 1
		
		index = m.values().index( max(m.values()) )
		ncols = int(m.keys()[index])
		
		
		return ncols;
	"""
		This function will clean records of a given row by removing non-ascii characters
		@pre self.xchar is not None
	"""
	def format (self,row):
		
		if isinstance(row,list) == False:
			#
			# We've observed sometimes fields contain delimiter as a legitimate character, we need to be able to account for this and not tamper with the field values (unless necessary)
			cols = self.split(row)
			#cols = row.split(self.xchar)
		else:
			cols = row ;
		return [ re.sub('[^\x00-\x7F,\n,\r,\v,\b,]',' ',col.strip()).strip().replace('"','') for col in cols]
		
		#if isinstance(row,list) == False:
		#	return (self.xchar.join(r)).format('utf-8')
		#else:
		#	return r
	"""
		This function performs a split of a record and tries to attempt to preserve the integrity of the data within i.e accounting for the double quotes.
		@pre : self.xchar is not None
	""" 
	def split (self,row):

		pattern = "".join(["(?:^|",self.xchar,")(\"(?:[^\"]+|\"\")*\"|[^",self.xchar,"]*)"])
		return re.findall(pattern,row.replace('\n',''))
		
class Writer:
	
	def format(self,row,xchar):
		if xchar is not None and isinstance(row,list):
			return xchar.join(row)+'\n'
		elif xchar is None and isinstance(row,dict):
			row = json.dumps(row)
		return row
	"""
		It is important to be able to archive data so as to insure that growth is controlled
		Nothing in nature grows indefinitely neither should data being handled.
	"""
	def archive(self):
		pass
	def flush(self):
		pass
	
"""
  This class is designed to read data from an Http request file handler provided to us by flask
  The file will be heald in memory and processed accordingly
  NOTE: This is inefficient and can crash a micro-instance (becareful)
"""
class HttpRequestReader(Reader):
	def __init__(self,**params):
		self.file_length = 0
		try:
			
			#self.file = params['file']	
			#self.file.seek(0, os.SEEK_END)
			#self.file_length = self.file.tell()
			
			#print 'size of file ',self.file_length
			self.content = params['file'].readlines()
			self.file_length = len(self.content)
		except Exception, e:
			print "Error ... ",e
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
		
"""
	This class is designed to write data to a session/cookie
"""
class HttpSessionWriter(Writer):
	"""
		@param key	required session key
	"""
	def __init__(self,**params):
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
		
"""
  This class is designed to read data from disk (location on hard drive)
  @pre : isready() == True
"""
class DiskReader(Reader) :
	"""
		@param	path	absolute path of the file to be read
	"""
	def __init__(self,**params):
		Reader.__init__(self)
		self.path = params['path'] ;

	def isready(self):
		return os.path.exists(self.path) 
	"""
		This function reads the rows from a designated location on disk
		@param	size	number of rows to be read, -1 suggests all rows
	"""
	def read(self,size=-1):
		f = open(self.path,'rU') 
		i = 1
		for row in f:
			
			i += 1
			if size == i:
				break
			yield row
		f.close()
"""
	This function writes output to disk in a designated location
"""
class DiskWriter(Writer):
	def __init__(self,**params):
		if 'path' in params:
			self.path = params['path']
		else:
			self.path = None
		if 'name' in params:
			self.name = params['name'];
		else:
			self.name = None
		if os.path.exists(self.path) == False:
			os.mkdir(self.path)
	"""
		This function determines if the class is ready for execution or not
		i.e it determines if the preconditions of met prior execution
	"""
	def isready(self):
		
		p =  self.path is not None and os.path.exists(self.path)
		q = self.name is not None 
		return p and q
	"""
		This function writes a record to a designated file
		@param	label	<passed|broken|fixed|stats>
		@param	row	row to be written
	"""
	def write(self,**params):
		label 	= params['label']
		row 	= params['row']
		xchar = None
		if 'xchar' is not None:
			xchar 	= params['xchar']
		path = ''.join([self.path,os.sep,label])
		if os.path.exists(path) == False:
			os.mkdir(path) ;
		path = ''.join([path,os.sep,self.name]) 
		f = open(path,'a')
		row = self.format(row,xchar);
		f.write(row)
		f.close()
"""
	This class hierarchy is designed to handle interactions with a queue server using pika framework (our tests are based on rabbitmq)
"""
class MessageQueue:
	def __init__(self,**params):
		self.host= params['host']
		self.uid = params['uid']
		self.qid = params['qid']
	
	def isready(self):
		#self.init()
		resp =  self.connection is not None and self.connection.is_open
		self.close()
		return resp
	def close(self):
            if self.connection.is_closed == False :
		self.channel.close()
		self.connection.close()
"""
	This class is designed to publish content to an AMQP (Rabbitmq)
	The class will rely on pika to implement this functionality

	We will publish information to a given queue for a given exchange
"""

class QueueWriter(MessageQueue,Writer):
	def __init__(self,**params):
		#self.host= params['host']
		#self.uid = params['uid']
		#self.qid = params['queue']
		MessageQueue.__init__(self,**params);
		
		
	def init(self,label=None):
		properties = pika.ConnectionParameters(host=self.host)
		self.connection = pika.BlockingConnection(properties)
		self.channel	= self.connection.channel()
		self.info = self.channel.exchange_declare(exchange=self.uid,type='direct',durable=True)
		if label is None:
			self.qhandler = self.channel.queue_declare(queue=self.qid,durable=True)	
		else:
			self.qhandler = self.channel.queue_declare(queue=label,durable=True)
		
		self.channel.queue_bind(exchange=self.uid,queue=self.qhandler.method.queue) 
		


	"""
		This function writes a stream of data to the a given queue
		@param object	object to be written (will be converted to JSON)
		@TODO: make this less chatty
	"""
	def write(self,**params):
		xchar = None
		if  'xchar' in params:
			xchar = params['xchar']
		object = self.format(params['row'],xchar)
		
		label	= params['label']
		self.init(label)
		_mode = 2
		if isinstance(object,str):
			stream = object
			_type = 'text/plain'
		else:
			stream = json.dumps(object)
			if 'type' in params :
				_type = params['type']
			else:
				_type = 'application/json'

		self.channel.basic_publish(
			exchange=self.uid,
			routing_key=label,
			body=stream,
			properties=pika.BasicProperties(content_type=_type,delivery_mode=_mode)
		);
		self.close()

	def flush(self,label):
		self.init(label)
		_mode = 1  #-- Non persistent
		self.channel.queue_delete( queue=label);
		self.close()
		
"""
	This class will read from a queue provided an exchange, queue and host
	@TODO: Account for security and virtualhosts
"""
class QueueReader(MessageQueue,Reader):
	"""
		@param	host	host
		@param	uid	exchange identifier
		@param	qid	queue identifier
	"""
	def __init__(self,**params):
		#self.host= params['host']
		#self.uid = params['uid']
		#self.qid = params['qid']
		MessageQueue.__init__(self,**params);
		if 'durable' in params :
			self.durable = True
		else:
			self.durable = False
		self.size = -1
		self.data = {}
	def init(self,qid):
		
		properties = pika.ConnectionParameters(host=self.host)
		self.connection = pika.BlockingConnection(properties)
		self.channel	= self.connection.channel()
		self.channel.exchange_declare(exchange=self.uid,type='direct',durable=True)

		self.info = self.channel.queue_declare(queue=qid,durable=True)
	


	"""
		This is the callback function designed to process the data stream from the queue

	"""
	def callback(self,channel,method,header,stream):
                
		r = []
		if re.match("^\{|\[",stream) is not None:
			r = json.loads(stream)
		else:
			
			r = stream
		
		qid = self.info.method.queue
		if qid not in self.data :
			self.data[qid] = []
		
		self.data[qid].append(r)
		#
		# We stop reading when the all the messages of the queue are staked
		#
		if self.size == len(self.data[qid]) or len(self.data[qid]) == self.info.method.message_count:		
			self.close()

	"""
		This function will read, the first message from a queue
		@TODO: 
		Implement channel.basic_get in order to retrieve a single message at a time
		Have the number of messages retrieved be specified by size (parameter)
	"""
	def read(self,size=-1):
		r = {}
		self.size = size
		#
		# We enabled the reader to be able to read from several queues (sequentially for now)
		# The qid parameter will be an array of queues the reader will be reading from
		#
		if isinstance(self.qid,basestring) :
                    self.qid = [self.qid]
		for qid in self.qid:
			self.init(qid)
			# r[qid] = []
			
			if self.info.method.message_count > 0:
				
				self.channel.basic_consume(self.callback,queue=qid,no_ack=False);
				self.channel.start_consuming()
			else:
				
				pass
				#self.close()
			# r[qid].append( self.data)
		
		return self.data
class QueueListener(QueueReader):
	def init(self,qid):
		properties = pika.ConnectionParameters(host=self.host)
		self.connection = pika.BlockingConnection(properties)
		self.channel	= self.connection.channel()
		self.channel.exchange_declare(exchange=self.uid,type='direct',durable=True )

		self.info = self.channel.queue_declare(passive=True,exclusive=True,queue=qid)
		
		self.channel.queue_bind(exchange=self.uid,queue=self.info.method.queue,routing_key=qid)
		#self.callback = callback
	def read(self):
    	
		self.init(self.qid)
		self.channel.basic_consume(self.callback,queue=self.qid,no_ack=True);
		self.channel.start_consuming()
    		
"""
	This class is designed to write output as sql insert statements
	The class will inherit from DiskWriter with minor adjustments
	@TODO: Include script to create the table if need be using the upper bound of a learner
"""
class SQLDiskWriter(DiskWriter):
	def __init__(self,**args):
		DiskWriter.__init__(self,**args)
		self.tablename = re.sub('\..+$','',self.name).replace(' ','_')
	"""
		@param label
		@param row
		@param xchar
	"""
	def write(self,**args):
		label	= args['label']
		row = args['row']
		
		if label == 'usable':
			values = "','".join([col.replace('"','').replace("'",'') for col in row])
			row = "".join(["INSERT INTO :table VALUES('",values,"');\n"]).replace(':table',self.tablename)

			args['row']  = row
		DiskWriter.write(self,**args)
class Couchdb:
	"""
		@param	uri		host & port reference
		@param	uid		user id involved

		@param	dbname		database name (target)
	"""
	def __init__(self,**args):
		uri 		= args['uri']
		self.uid 	= args['uid']
		dbname		= args['dbname']
		self.server 	= Server(uri=uri) 
		self.dbase	= self.server.get_db(dbname)
		if self.dbase.doc_exist(self.uid) == False:
			self.dbase.save_doc({"_id":self.uid})
	"""
		Insuring the preconditions are met for processing
	"""
	def isready(self):
		p = self.server.info() != {}
		if p == False or self.dbase.dbname not in self.server.all_dbs():
			return False
		#
		# At this point we are sure that the server is connected
		# We are also sure that the database actually exists
		#
		q = self.dbase.doc_exist(self.uid)
		if q == False:
			return False
		return True
	def view(self,id,**args):
		r =self.dbase.view(id,**args)
		r = r.all()		
		return r[0]['value'] if len(r) > 0 else []
		
"""
	This function will read an attachment from couchdb and return it to calling code. The attachment must have been placed before hand (otherwise oops)
	@T: Account for security & access control
"""
class CouchdbReader(Couchdb,Reader):
	"""
		@param	filename	filename (attachment)
	"""
	def __init__(self,**args):
		#
		# setting the basic parameters for 
		Couchdb.__init__(self,**args)
		if 'filename' in args :
			self.filename 	= args['filename']
		else:
			self.filename = None

	def isready(self):
		#
		# Is the basic information about the database valid
		#
		p = Couchdb.isready(self)
		
		if p == False:
			return False
		#
		# The database name is set and correct at this point
		# We insure the document of the given user has the requested attachment.
		# 
		
		doc = self.dbase.get(self.uid)
		
		if '_attachments' in doc:
			r = self.filename in doc['_attachments'].keys()
			
		else:
			r = False
		
		return r	
	def stream(self):
		content = self.dbase.fetch_attachment(self.uid,self.filename).split('\n') ;
		i = 1
		for row in content:
			yield row
			if size > 0 and i == size:
				break
			i = i + 1
		
	def read(self,size=-1):
		if self.filename is not None:
			self.stream()
		else:
			return self.basic_read()
	def basic_read(self):
		document = self.dbase.get(self.uid) 
		del document['_id'], document['_rev']
		return document
"""
	This class will write on a couchdb document provided a scope
	The scope is the attribute that will be on the couchdb document
"""
class CouchdbWriter(Couchdb,Writer):		
	"""
		@param	uri		host & port reference
		@param	uid		user id involved
		@param	filename	filename (attachment)
		@param	dbname		database name (target)
	"""
	def __init__(self,**args):

		Couchdb.__init__(self,**args)
		uri 		= args['uri']
		self.uid 	= args['uid']
		if 'filename' in args:
			self.filename 	= args['filename']
		else:
			self.filename = None
		dbname		= args['dbname']
		self.server 	= Server(uri=uri) 
		self.dbase	= self.server.get_db(dbname)
		#
		# If the document doesn't exist then we should create it
		#

	"""
		write a given attribute to a document database
		@param	label	scope of the row repair|broken|fixed|stats
		@param	row	row to be written
	"""
	def write(self,**params):
		
		document = self.dbase.get(self.uid)
		label = params['label']
		row	= params['row']
		if label not in document :
			document[label] = []
		document[label].append(row)
		self.dbase.save_doc(document)
	def flush(self,**params) :
		
		size = params['size'] if 'size' in params else 0
		has_changed = False	
		document = self.dbase.get(self.uid)
		for key in document:
			if key not in ['_id','_rev','_attachments'] :
				content = document[key]
			else:
				continue
			if isinstance(content,list) and size > 0:
				index = len(content) - size
				content = content[index:]
				document[key] = content
				
			else:
				document[key] = {}
				has_changed = True
		
		self.dbase.save_doc(document)
			
	def archive(self,params=None):
		document = self.dbase.get(self.uid)
		content = {}
		_doc = {}
		for id in document:
			if id in ['_id','_rev','_attachments'] :
				_doc[id] = document[id]
			else:
				content[id] = document[id]
				
		content = json.dumps(content)	
		document= _doc
		now = str(datetime.today())
		
		name = '-'.join([document['_id'] , now,'.json'])			
		self.dbase.save_doc(document)
		self.dbase.put_attachment(document,content,name,'application/json')
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
			print e
	def buckets(self):
        	"""
			This function is a wrapper around the bucket list of buckets for s3

		"""
		return self.s3.get_all_buckets()
		
        	
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
"""
	This class acts as a factory to be able to generate an instance of a Reader/Writer
	Against a Queue,Disk,Cloud,Couchdb 
	The class doesn't enforce parameter validation, thus any error with the parameters sent will result in a null Object
"""
class Factory:
	def instance(self,**args):
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
		except Exception,e:
			print ['Error ',e]
		return anObject
class s3Writer(s3,Writer) :
        def __init__(self,args) :
        	s3.__init__(self,args)
	
"""
	This class implements a data-source handler that is intended to be used within the context of data processing, it allows to read/write anywhere transparently.
	The class is a facade to a heterogeneous class hierarchy and thus simplifies how the calling code interacts with the class hierarchy
"""
class DataSource:
	def __init__(self,sourceType='Disk',outputType='Disk',params={}):
		self.Input = DataSourceFactory.instance(type=sourceType,args=params)
		self.Output= DataSourceFactory.instance(type=outputType,args=params)
	def read(self,size=-1):
		return self.Input.read(size)
	def write(self,**args):
		self.Output.write(**args)
conf = json.loads(open('config.json').read())
#x = s3Reader( dict(conf,**{'bucket':'com.phi.sample.data','file':'Sample-Spreadsheet-5000-rows.csv'}))
x = s3Reader(conf)
print conf
print x.bucket.get_all_keys()
# r = x.read()
# for item in r :
# 	print item
#print buckets[1].get_key('Sample-Spreadsheet-5000-rows.csv')
