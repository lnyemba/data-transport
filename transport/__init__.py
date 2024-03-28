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
We separated reads from writes to mitigate accidents associated with writes.
Source Code is available under MIT License:    
    https://healthcareio.the-phi.com/data-transport
    https://hiplab.mc.vanderbilt.edu/git/hiplab/data-transport
"""
import numpy as np
from transport import sql, nosql, cloud, other
import pandas as pd
import json
import os
from info import __version__,__author__

PROVIDERS = {}
def init():
    global PROVIDERS
    for _module in [cloud,sql,nosql,other] :
        for _provider_name in dir(_module) :
            if _provider_name.startswith('__') :
                continue
            PROVIDERS[_provider_name] = {'module':getattr(_module,_provider_name),'type':_module.__name__}
# print ([ {name:getattr(sql,name)} for name in dir(sql) if not name.startswith('__')])

def instance (**_args):
    """
    type:
    read: true|false (default true)
    auth_file
    """
    global PROVIDERS
    if 'auth_file' in _args:
        if os.path.exists(_args['auth_file']) :
            f = open(_args['auth_file'])
            _args = dict (_args,** json.loads(f.read()) )
            f.close()
        else:
            filename = _args['auth_file']
            raise Exception(f" {filename} was not found or is invalid")
    if _args['provider'] in PROVIDERS :
        _info = PROVIDERS[_args['provider']]
        _module = _info['module']
        if 'context' in _args :
            _context = _args['context']
        else:
            _context = 'read'
        _pointer = getattr(_module,'Reader') if _context == 'read' else getattr(_module,'Writer')
        return _pointer (**_args)
        pass
    else:
        raise Exception ("Missing or Unknown provider")
    pass
def supported ():
    _info = {}
    for _provider in PROVIDERS :
        _item = PROVIDERS[_provider]
        if _item['type'] not in _info :
            _info[_item['type']] = []
        _info[_item['type']].append(_provider)
    _df  = pd.DataFrame()
    for _id in _info :
        if not _df.shape[0] :
            _df = pd.DataFrame(_info[_id],columns=[_id.replace('transport.','')])
        else:
            _df = pd.DataFrame(_info[_id],columns=[_id.replace('transport.','')]).join(_df, how='outer')
    return _df.fillna('')
class factory :
    pass
factory.instance = instance
init()
# if __name__ == '__main__' :
# # if not PROVIDERS :
#     init()
#     print (list(PROVIDERS.keys()))
#     pgr = instance(provider='postgresql',database='io',table='foo',write=True)
#     print (pgr.read())
#     print ()
#     print (supported())