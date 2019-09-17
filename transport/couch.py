"""
Data-Transport
Steve L. Nyemba, The Phi Technology

This file is a wrapper around couchdb using IBM Cloudant SDK that has an interface to couchdb

"""
import cloudant
import json
from common import Reader,Writer
class Couch:
	"""
	This class is a wrapper for read/write against couchdb. The class captures common operations for read/write.
		@param	url		host & port reference
		@param	doc		user id involved
		@param	dbname		database name (target)
	"""
	def __init__(self,**args):
		url 		= args['url']
		self.uid 	= args['doc']
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
			doc = cloudant.document.Document(self.dbase,self.uid) #self.dbase.get(self.uid)
			if not doc.exists():
				doc = self.dbase.create_document({"_id":self.uid})
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
		doc = cloudant.document.Document(self.dbase,self.uid)
		# q = self.dbase.all_docs(key=self.uid)['rows'] 
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
		
		

		
class CouchReader(Couch,Reader):
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

	# def isready(self):
	# 	#
	# 	# Is the basic information about the database valid
	# 	#
	# 	p = Couchdb.isready(self)
		
	# 	if p == False:
	# 		return False
	# 	#
	# 	# The database name is set and correct at this point
	# 	# We insure the document of the given user has the requested attachment.
	# 	# 
		
	# 	doc = self.dbase.get(self.uid)
		
	# 	if '_attachments' in doc:
	# 		r = self.filename in doc['_attachments'].keys()
			
	# 	else:
	# 		r = False
		
	# 	return r	
	def stream(self):
		#
		# @TODO Need to get this working ...
		#
		document = cloudant.document.Document(self.dbase,self.uid)
		# content = self.dbase.fetch_attachment(self.uid,self.filename).split('\n') ;
		content = self.get_attachment(self.filename)
		for row in content:
			yield row
		
	def read(self,size=-1):
		if self.filename is not None:
			self.stream()
		else:
			return self.basic_read()
	def basic_read(self):
		document = cloudant.document.Document(self.dbase,self.uid)
		
		# document = self.dbase.get(self.uid)
		if document.exists() :			
			document.fetch()
			document = dict(document)
			del document['_rev']
		else:
			document = {}
		return document

class CouchWriter(Couch,Writer):		
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

		Couch.__init__(self,**args)

	def write(self,**params):
		"""
			write a given attribute to a document database
			@param	label	scope of the row repair|broken|fixed|stats
			@param	row	row to be written
		"""
		
		# document = self.dbase.get(self.uid)
		document = cloudant.document.Document(self.dbase,self.uid) #.get(self.uid)
		if document.exists() is False :
			document = self.dbase.create_document({"_id":self.uid})
		label = params['label']
		row	= params['row']
		if label not in document :
			document[label] = []
		document[label].append(row)
		document.save()
		# self.dbase.bulk_docs([document])
		# self.dbase.save_doc(document)
			
	def archive(self,params=None):
		"""
		This function will archive the document onto itself. 		
		"""
		# document = self.dbase.all_docs(self.uid,include_docs=True)
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
		# self.dbase.bulk_docs([document])
		# self.dbase.put_attachment(document,content,name,'application/json')
		document.put_attachment(self.dbase,name,'application/json',content)
		document.save()
