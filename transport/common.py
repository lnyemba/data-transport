"""
Data Transport - 1.0
Steve L. Nyemba, The Phi Technology LLC

This module is designed to serve as a wrapper to a set of supported data stores :
    - couchdb
    - mongodb
    - Files (character delimited)
    - Queues (RabbmitMq)
    - Session (Flask)
    - s3
The supported operations are read/write and providing meta data to the calling code
Requirements :
	pymongo
	boto
	couldant

"""
__author__ = 'The Phi Technology'
import numpy as np
import json
import importlib 
# import couch
# import mongo
class Reader:
	def __init__(self):
		self.nrows = 0
		self.xchar = None
		
	def row_count(self):		
		content = self.read()
		return np.sum([1 for row in content])
	def delimiter(self,sample):
		"""
			This function determines the most common delimiter from a subset of possible delimiters. 
			It uses a statistical approach (distribution) to guage the distribution of columns for a given delimiter
			
			:sample sample  string/content expecting matrix i.e list of rows
		"""
		
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
	def col_count(self,sample):
		"""
		This function retirms the number of columns of a given sample
		@pre self.xchar is not None
		"""
		
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
	def format (self,row):
		"""
			This function will clean records of a given row by removing non-ascii characters
			@pre self.xchar is not None
		"""
		
		if isinstance(row,list) == False:
			#
			# We've observed sometimes fields contain delimiter as a legitimate character, we need to be able to account for this and not tamper with the field values (unless necessary)
			cols = self.split(row)
			#cols = row.split(self.xchar)
		else:
			cols = row ;
		return [ re.sub('[^\x00-\x7F,\n,\r,\v,\b,]',' ',col.strip()).strip().replace('"','') for col in cols]
		
	def split (self,row):
		"""
			This function performs a split of a record and tries to attempt to preserve the integrity of the data within i.e accounting for the double quotes.
			@pre : self.xchar is not None
		""" 

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

# class factory :
# 	@staticmethod
# 	def instance(**args):
# 		"""
# 		This class will create an instance of a transport when providing 
# 		:type	name of the type we are trying to create
# 		:args	The arguments needed to create the instance
# 		"""
# 		source = args['type']		
# 		params = args['args']
# 		anObject = None
		
# 		if source in ['HttpRequestReader','HttpSessionWriter']:
# 			#
# 			# @TODO: Make sure objects are serializable, be smart about them !!
# 			#
# 			aClassName = ''.join([source,'(**params)'])


# 		else:
			
# 			stream = json.dumps(params)
# 			aClassName = ''.join([source,'(**',stream,')'])
# 		try:
# 			anObject = eval( aClassName)
# 			#setattr(anObject,'name',source)
# 		except Exception,e:
# 			print ['Error ',e]
# 		return anObject