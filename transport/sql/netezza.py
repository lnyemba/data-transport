import nzpy as nz   
from transport.sql.common import BaseReader, BaseWriter, template as _template


def template ():
    return dict(_template(),**{'port':5480})
class Netezza:
    def get_provider(self):
        return 'netezza+nzpy'
    def get_default_port(self):
        return '5480'
    
class Reader(Netezza,BaseReader) :
    def __init__(self,**_args):        
        super().__init__(**_args)
class Writer(Netezza,BaseWriter):
    def __init__(self,**_args):
        super().__init__(**_args)