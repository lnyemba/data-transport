
from transport.sql.common import BaseReader , BaseWriter, template as _template
from psycopg2.extensions import register_adapter, AsIs
import numpy as np

register_adapter(np.int64, AsIs)

def template ():
    return dict(_template(),**{'port':5432,'chunksize':10000})

class PG:
    def __init__(self,**_args):
        super().__init__(**_args)
    def get_provider(self):
        return "postgresql"
        
    def get_default_port(self):
        return "5432"        
class Reader(PG,BaseReader) :
    def __init__(self,**_args):        
        super().__init__(**_args)
class Writer(PG,BaseWriter):
    def __init__(self,**_args):
        super().__init__(**_args)

