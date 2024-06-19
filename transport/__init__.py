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
from info import __version__,__author__,__email__,__license__,__app_name__,__whatsnew__
from transport.iowrapper import IWriter, IReader, IETL
from transport.plugins import PluginLoader
from transport import providers
import copy 
from transport import registry

PROVIDERS = {}

def init():
    global PROVIDERS
    for _module in [cloud,sql,nosql,other] :
        for _provider_name in dir(_module) :
            if _provider_name.startswith('__') or _provider_name == 'common':
                continue
            PROVIDERS[_provider_name] = {'module':getattr(_module,_provider_name),'type':_module.__name__}
def _getauthfile (path) :
    f = open(path)
    _object = json.loads(f.read())
    f.close()
    return _object
def instance (**_args):
    """
    This function returns an object of to read or write from a supported database provider/vendor
    @provider   provider
    @context    read/write (default is read)
    @auth_file: Optional if the database information provided is in a file. Useful for not sharing passwords
    kwargs      These are arguments that are provider/vendor specific
    """
    global PROVIDERS
    # if not registry.isloaded () :
    #     if ('path' in _args and registry.exists(_args['path'] )) or registry.exists():
    #         registry.load() if 'path' not in _args else registry.load(_args['path'])
    #         print ([' GOT IT'])
    # if 'label' in _args and registry.isloaded():
    #     _info = registry.get(_args['label'])
    #     if _info :
    #         #
    #         _args = dict(_args,**_info)

    if 'auth_file' in _args:
        if os.path.exists(_args['auth_file']) :
            #
            # @TODO: add encryption module and decryption to enable this to be secure
            #

            f = open(_args['auth_file'])
            #_args = dict (_args,** json.loads(f.read()) )
            #
            # we overrite file parameters with arguments passed
            _args = dict (json.loads(f.read()),**_args )
            f.close()
        else:
            filename = _args['auth_file']
            raise Exception(f" {filename} was not found or is invalid")
    if 'provider' not in _args and 'auth_file' not in _args :
        if not registry.isloaded () :
            if ('path' in _args and registry.exists(_args['path'] )) or registry.exists():
                registry.load() if 'path' not in _args else registry.load(_args['path'])
        if 'label' in _args and registry.isloaded():
            _info = registry.get(_args['label'])
            
            if _info :
                #
                # _args = dict(_args,**_info)
                _args = dict(_info,**_args) #-- we can override the registry parameters with our own arguments

    if 'provider' in _args and _args['provider'] in PROVIDERS :
        _info = PROVIDERS[_args['provider']]
        _module = _info['module']
        if 'context' in _args :
            _context = _args['context']
        else:
            _context = 'read'
        _pointer    = getattr(_module,'Reader') if _context == 'read' else getattr(_module,'Writer')
        _agent      = _pointer (**_args)
        #
        loader = None
        
        #
        # @TODO:
        # define a logger object here that will used by the wrapper
        # this would allow us to know what the data-transport is doing and where/how it fails
        #

        # if 'plugins' in _args :
        #     _params = _args['plugins']

        #     if 'path' in _params and 'names' in _params :
        #         loader = PluginLoader(**_params)
        #     elif type(_params) == list:
        #         loader = PluginLoader()
        #         for _delegate in _params :
        #             loader.set(_delegate)
        
        loader = None if 'plugins' not in _args else _args['plugins']
        return IReader(_agent,loader) if _context == 'read' else IWriter(_agent,loader)

    else:
        #
        # We can handle the case for an ETL object
        #
        raise Exception ("Missing or Unknown provider")
    pass
class get :
    """
    This class is just a wrapper to make the interface (API) more conversational and easy to understand
    """
    @staticmethod
    def reader (**_args):
        if not _args :
            _args['label'] = 'default'
        _args['context'] = 'read'
        return instance(**_args)
    @staticmethod
    def writer(**_args):
        """
        This function is a wrapper that will return a writer to a database. It disambiguates the interface
        """
        if not _args :
            _args['label'] = 'default'
        _args['context'] = 'write'
        return instance(**_args)
    @staticmethod
    def etl (**_args):
        if 'source' in _args and 'target' in _args :
            return IETL(**_args)
        else:
            raise Exception ("Malformed input found, object must have both 'source' and 'target' attributes")
    
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
