import json
import numpy as np
from datetime import datetime

class IEncoder (json.JSONEncoder):
	def default (self,object):
		if type(object) == np.integer :
			return int(object)
		elif type(object) == np.floating:
			return float(object)
		elif type(object) == np.ndarray :
			return object.tolist()
		elif type(object) == datetime :
			return object.isoformat()
		else:
			return super(IEncoder,self).default(object)


