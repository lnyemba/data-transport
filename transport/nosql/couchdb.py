"""
Data-Transport
Steve L. Nyemba, The Phi Technology

This file is a wrapper around couchdb using IBM Cloudant SDK that has an interface to couchdb

"""
import cloudant
import json
import sys
# from transport.common import Reader, Writer
from datetime import datetime

def template():
	return {'dbname':'database','doc':'document','username':'username','password':'password','url':'url-with-port'}

class Couch:
	"""
	This class is a wrapper for read/write against couchdb. The class captures common operations for read/write.
		@param	url		host & port reference default http://localhost:5984
		@param	doc		user id involved
		@param	dbname		database name (target)
	"""
	def __init__(self,**args):
		url 		= args['url'] if 'url' in args else 'http://localhost:5984'
		self._id 	= args['doc']
		dbname		= args['dbname']
		if 'username' not in args and 'password' not in args :
			self.server 	= cloudant.CouchDB(None,None,url=url)
		else:
			self.server = cloudant.CouchDB(args['username'],args['password'],url=url)
		self.server.connect()

		if dbname in self.server.all_dbs() :
			self.dbase	= self.server.get(dbname,dbname,True)
			#
			# @TODO Check if the database exists ...
			#
			doc = cloudant.document.Document(self.dbase,self._id) #self.dbase.get(self._id)
			if not doc.exists():
				doc = self.dbase.create_document({"_id":self._id})
				doc.save()
		else:
			self.dbase = None
	"""
		Insuring the preconditions are met for processing
	"""
	def isready(self):
		p = self.server.metadata() != {}
		if p == False or not self.dbase:
			return False
		#
		# At this point we are sure that the server is connected
		# We are also sure that the database actually exists
		#
		doc = cloudant.document.Document(self.dbase,self._id)
		# q = self.dbase.all_docs(key=self._id)['rows'] 
		# if not q :
		if not doc.exists():
			return False
		return True
	
	def view(self,**args):
		"""
		The function will execute a view (provivded a user is authenticated)
		:id	design 	document _design/xxxx (provide full name with _design prefix)
		:view_name	name of the view i.e 
		:key(s)		key(s) to be used to filter the content
		"""
		document = cloudant.design_document.DesignDocument(self.dbase,args['id'])
		document.fetch()
		params = {'group_level':1,'group':True}
		if 'key' in  args :
			params ['key'] = args['key']
		elif 'keys' in args :
			params['keys'] = args['keys']
		return document.get_view(args['view_name'])(**params)['rows']
		
		

		
class Reader(Couch):
	"""
		This function will read an attachment from couchdb and return it to calling code. The attachment must have been placed before hand (otherwise oops)
		@T: Account for security & access control
	"""
	def __init__(self,**args):
		"""
			@param	filename	filename (attachment)
		"""
		#
		# setting the basic parameters for 
		Couch.__init__(self,**args)
		if 'filename' in args :
			self.filename 	= args['filename']
		else:
			self.filename = None

	
	def stream(self):
		#
		# @TODO Need to get this working ...
		#
		document = cloudant.document.Document(self.dbase,self._id)
		# content = self.dbase.fetch_attachment(self._id,self.filename).split('\n') ;
		content = self.get_attachment(self.filename)
		for row in content:
			yield row
		
	def read(self,**args):
		if self.filename is not None:
			self.stream()
		else:
			return self.basic_read()
	def basic_read(self):
		document = cloudant.document.Document(self.dbase,self._id)
		
		# document = self.dbase.get(self._id)
		if document.exists() :			
			document.fetch()
			document = dict(document)
			del document['_rev']
		else:
			document = {}
		return document

class Writer(Couch):		
	"""
		This class will write on a couchdb document provided a scope
		The scope is the attribute that will be on the couchdb document
	"""
	def __init__(self,**args):
		"""
			@param	uri		host & port reference
			@param	uid		user id involved
			@param	filename	filename (attachment)
			@param	dbname		database name (target)
		"""

		super().__init__(self,**args)
	def set (self,info):
		document  = cloudant.document.Document(self.dbase,self._id)
		if document.exists() :
			keys = list(set(document.keys()) - set(['_id','_rev','_attachments']))
			for id in keys :
				document.field_set(document,id,None)
			for id in info :
				value = info[id]
				document.info(document,id,value)
			
			document.save()
			pass
		else:
			_document = dict({"_id":self._id},**args)
			document.create_document(_document)
	def write(self,info):
		"""
			write a given attribute to a document database
			@info	object to be written to the to an attribute. this 
		"""
		
		# document = self.dbase.get(self._id)
		document = cloudant.document.Document(self.dbase,self._id) #.get(self._id)
		if document.exists() is False :
			document = self.dbase.create_document({"_id":self._id})
		# label = params['label']
		# row	= params['row']
		# if label not in document :
		# 	document[label] = []
		# document[label].append(row)
		for key in info :
			if key in document and type(document[key]) == list :
				document[key] += info[key]
			else:
				document[key] = info[key]
				
		document.save()
		# self.dbase.bulk_docs([document])
		# self.dbase.save_doc(document)
	
	def upload(self,**args):
		"""
		:param	name	name of the file to be uploaded
		:param	data	content of the file (binary or text)
		:param	content_type	(default)
		"""
		mimetype = args['content_type'] if 'content_type' in args else 'text/plain'
		document = cloudant.document.Document(self.dbase,self.uid)		
		document.put_attachment(self.dbase,args['filename'],mimetype,args['content'])
		document.save()

	def archive(self,params=None):
		"""
		This function will archive the document onto itself. 		
		"""
		# document = self.dbase.all_docs(self._id,include_docs=True)
		document = cloudant.document.Document(self.dbase,self.filename)
		document.fetch()
		content = {}
		# _doc = {}
		for id in document:
			if  id not in ['_id','_rev','_attachments'] :
				content[id] = document[id]
				del document[id]
				
		content = json.dumps(content)	
		# document= _doc
		now = str(datetime.today())
		
		name = '-'.join([document['_id'] , now,'.json'])	
		self.upload(filename=name,data=content,content_type='application/json')		
		# self.dbase.bulk_docs([document])
		# self.dbase.put_attachment(document,content,name,'application/json')
		# document.put_attachment(self.dbase,name,'application/json',content)
		# document.save()
