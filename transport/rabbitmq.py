"""
Data Transport - 1.0
Steve L. Nyemba, The Phi Technology LLC

This file is a wrapper around rabbitmq server for reading and writing content to a queue (exchange)

"""
import pika
from datetime import datetime
import re
import json
import os
import sys
if sys.version_info[0] > 2 :
	from transport.common import Reader, Writer
else:
	from common import Reader, Writer
import json
from multiprocessing import RLock
class MessageQueue:
	"""
		This class hierarchy is designed to handle interactions with a queue server using pika framework (our tests are based on rabbitmq)
		:host	
		:xid	identifier of the exchange
		:qid	identifier of the queue
	"""
	def __init__(self,**params):
		self.host= 'localhost' if 'host' not in params else params['host']	#-- location of the queue server
		self.port= 5672 if 'port' not in params else params['port']
		self.virtual_host = '/' if 'vhost' not in params else params['vhost']
		self.exchange = params['exchange']	if 'exchange' in params else 'amq.direct' #-- exchange
		self.queue = params['queue'] if 'queue' in params else 'demo'	
		self.connection = None
		self.channel 	= None
		
		self.name 	= self.__class__.__name__.lower() if 'name' not in params else params['name']
		
		username = password = None
		if 'username' in params :
			username = params['username']
			password = params['password']
		if 'auth_file' in params :
			_info = json.loads((open(params['auth_file'])).read())
			username=_info['username']
			password=_info['password']
			self.virtual_host = _info['virtual_host'] if 'virtual_host' in _info else self.virtual_host
			self.exchange = _info['exchange'] if 'exchange' in _info else self.exchange
			self.queue = _info['queue'] if 'queue' in _info else self.queue

		self.credentials= pika.PlainCredentials('guest','guest')
		if 'username' in params :
			self.credentials = pika.PlainCredentials(
				params['username'],
				('' if 'password' not in params else params['password'])
			)
	
	def init(self,label=None):
		properties = pika.ConnectionParameters(host=self.host,port=self.port,virtual_host=self.virtual_host,
		client_properties={'connection_name':self.name},
		credentials=self.credentials)
		self.connection = pika.BlockingConnection(properties)
		self.channel	= self.connection.channel()
		self.info = self.channel.exchange_declare(exchange=self.exchange,exchange_type='direct',durable=True)
		if label is None:
			self.qhandler = self.channel.queue_declare(queue=self.queue,durable=True)	
		else:
			self.qhandler = self.channel.queue_declare(queue=label,durable=True)
		
		self.channel.queue_bind(exchange=self.exchange,queue=self.qhandler.method.queue) 

	def isready(self):
		#self.init()
		resp =  self.connection is not None and self.connection.is_open
		# self.close()
		return resp
	def finalize(self):
		pass
	def close(self):
		if self.connection.is_closed == False :
			self.channel.close()
			self.connection.close()

class QueueWriter(MessageQueue,Writer):
	"""
		This class is designed to publish content to an AMQP (Rabbitmq)
		The class will rely on pika to implement this functionality

		We will publish information to a given queue for a given exchange
	"""
	def __init__(self,**params):
		#self.host= params['host']
		#self.exchange = params['uid']
		#self.queue = params['queue']
		MessageQueue.__init__(self,**params);
		self.init()
		
		

		



	def write(self,data,_type='text/plain'):
		"""
		This function writes a stream of data to the a given queue
		@param object	object to be written (will be converted to JSON)
		@TODO: make this less chatty
		"""
	
		stream = json.dumps(data) if isinstance(data,dict) else data
		self.channel.basic_publish(
			exchange=self.exchange,
			routing_key=self.queue,
			body=stream,
			properties=pika.BasicProperties(content_type=_type,delivery_mode=2)
		);
		# self.close()

	def flush(self):
		self.init()
		_mode = 1  #-- Non persistent
		self.channel.queue_delete( queue=self.queue);
		self.close()
		
class QueueReader(MessageQueue,Reader):
	"""
	This class will read from a queue provided an exchange, queue and host
	@TODO: Account for security and virtualhosts
	"""

	def __init__(self,**params):
		"""
			@param	host	host
			@param	uid	exchange identifier
			@param	qid	queue identifier
		"""

		#self.host= params['host']
		#self.exchange = params['uid']
		#self.queue = params['qid']
		MessageQueue.__init__(self,**params);
		# self.init()
		self.durable = False if 'durable' not in params else params['durable']
		# if 'durable' in params :
		# 	self.durable = True
		# else:
		# 	self.durable = False
		self.size = -1
		self.data = {}
	# def init(self,qid):
		
	# 	properties = pika.ConnectionParameters(host=self.host)
	# 	self.connection = pika.BlockingConnection(properties)
	# 	self.channel	= self.connection.channel()
	# 	self.channel.exchange_declare(exchange=self.exchange,type='direct',durable=True)

	# 	self.info = self.channel.queue_declare(queue=qid,durable=True)
	

	def callback(self,channel,method,header,stream):
		"""
			This is the callback function designed to process the data stream from the queue

		"""
				
		r = []
		# if re.match("^\{|\[",stream) is not None:
		if stream.startswith(b'{') or stream.startswith(b'['): 
			r = json.loads(stream)
		else:
			
			r = stream
		
		qid = self.qhandler.method.queue
		if qid not in self.data :
			self.data[qid] = []
		
		self.data[qid].append(r)
		#
		# We stop reading when the all the messages of the queue are staked
		#
		if self.size == len(self.data[qid]) or len(self.data[qid]) == self.info.method.message_count:		
			self.close()

	def read(self,**args):
		"""
		This function will read, the first message from a queue
		@TODO: 
			Implement channel.basic_get in order to retrieve a single message at a time
			Have the number of messages retrieved be specified by size (parameter)
		"""
		r = {}
		self.size = -1 if 'size' in args else int(args['size'])
		#
		# We enabled the reader to be able to read from several queues (sequentially for now)
		# The qid parameter will be an array of queues the reader will be reading from
		#
		if isinstance(self.queue,str) :
					self.queue = [self.queue]
		
		for qid in self.queue:
			self.init(qid)
			# r[qid] = []
			
			if self.qhandler.method.message_count > 0:
				
				self.channel.basic_consume(queue=qid,on_message_callback=self.callback,auto_ack=False);
				self.channel.start_consuming()
			else:
				
				pass
				#self.close()
			# r[qid].append( self.data)

		return self.data
class QueueListener(MessageQueue):
	lock = RLock()
	"""
	This class is designed to have an active listener (worker) against a specified Exchange/Queue
	It is initialized as would any other object and will require a callback function to address the objects returned.
	"""
	def __init__(self,**args):
		MessageQueue.__init__(self,**args)
		self.listen = self.read
		self.apply = args['apply'] if 'apply' in args else print
		self.lock = False if 'lock' not in args else args['lock']
	
	def finalize(self,channel,ExceptionReason):
		pass
	
	def callback(self,channel,method,header,stream) :
		_info= {}
		# if re.match("^\{|\[",stream) is not None:
		

		if stream.startswith(b"[") or stream.startswith(b"{"):
			_info = json.loads(stream)
		else:
			
			_info = stream
		#
		# At this point we should invoke the apply function  with a lock if need be
		# @TODO: Establish a vocabulary 
		
		if stream == b'QUIT' :
			# channel.exit()
			self.close()
		if self.lock == True :
			QueueListener.lock.acquire()
		try:
			#
			# In case the user has not specified a function to apply the data against, it will simply be printed
			#
			self.apply(_info)
		except Exception as e:
			pass
		if self.lock == True :
			QueueListener.lock.release()
	def read(self):
    	
		self.init(self.queue)
		
		self.channel.basic_consume(self.queue,self.callback,auto_ack=True);
		self.channel.start_consuming()
		
		
 
class Factory :
	@staticmethod
	def instance(**_args):
		"""
		:param count	number of workers
		:param apply	function workers		
		"""
		_apply = _args['apply']
		_count = _args['count']
		for i in np.arange(_count) :
			_name = _args['name'] if 'name' in _args else 'worker_'+str(i)
		transport.factory.instance(provider="rabbit",context="listener",apply=_apply,auth_file=_args['auth_file'])