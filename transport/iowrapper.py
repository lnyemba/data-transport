"""
This class is a wrapper around read/write classes of cloud,sql,nosql,other packages
The wrapper allows for application of plugins as pre-post conditions.
NOTE: Plugins are converted to a pipeline, so we apply a pipeline when reading or writing:
        - upon initialization we will load plugins
        - on read/write we apply a pipeline (if passed as an argument)
"""    
from transport.plugins import Plugin, PluginLoader
import transport
from transport import providers
from multiprocessing import Process
import time

import plugin_ix 

class IO:
    """
    Base wrapper class for read/write and support for logs
    """
    def __init__(self,**_args):
        _agent  = _args['agent']
        plugins = _args['plugins'] if 'plugins' in _args else None

        self._agent = _agent
        # self._ixloader = plugin_ix.Loader () #-- must indicate where the plugin registry file is 
        self._ixloader = plugin_ix.Loader (registry=plugin_ix.Registry(folder=transport.registry.REGISTRY_PATH))
        if plugins :
            self.init_plugins(plugins)

    def meta (self,**_args):
        if hasattr(self._agent,'meta') :
            return self._agent.meta(**_args)
        return []

    def close(self):
        if hasattr(self._agent,'close') :
            self._agent.close()
    # def apply(self):
    #     """
    #     applying pre/post conditions given a pipeline expression
    #     """
    #     for _pointer in self._plugins :
    #         _data = _pointer(_data)
    def apply(self,_query):
        if hasattr(self._agent,'apply') :
            return self._agent.apply(_query)
        return None
    def submit(self,_query):
        return self.delegate('submit',_query)
    def delegate(self,_name,_query):
        if hasattr(self._agent,_name) :
            pointer = getattr(self._agent,_name)
            return pointer(_query)
        return None
    def init_plugins(self,plugins):
        for _ref in plugins :
            self._ixloader.set(_ref)

class IReader(IO):
    """
    This is a wrapper for read functionalities
    """
    def __init__(self,**_args):
        super().__init__(**_args)
        
    def read(self,**_args):
        if 'plugins' in _args :
            self.init_plugins(_args['plugins'])

        _data = self._agent.read(**_args)
        # if self._plugins and self._plugins.ratio() > 0 :
        #     _data = self._plugins.apply(_data)
        #
        # output data 
        
        #
        # applying the the design pattern 
        _data = self._ixloader.visitor(_data)
        return _data
class IWriter(IO):
    def __init__(self,**_args): #_agent,pipeline=None):
        super().__init__(**_args) #_agent,pipeline)  
    def write(self,_data,**_args):
        # if 'plugins' in _args :
        #     self._init_plugins(_args['plugins'])
        if 'plugins' in _args :
            self.init_plugins(_args['plugins'])

        self._ixloader.visitor(_data)
        self._agent.write(_data,**_args)

#
# The ETL object in its simplest form is an aggregation of read/write objects
# @TODO: ETL can/should aggregate a writer as a plugin and apply it as a process

class IETL(IReader) :
    """
    This class performs an ETL operation by ineriting a read and adding writes as pipeline functions
    """
    def __init__(self,**_args):
        super().__init__(agent=transport.get.reader(**_args['source']),plugins=None)
        if 'target' in _args:
            self._targets = _args['target'] if type(_args['target']) == list else [_args['target']]
        else:
            self._targets = []
        self.jobs = []
        #
        # If the parent is already multiprocessing
        self._hasParentProcess = False if 'hasParentProcess' not in _args else _args['hasParentProcess']
    def read(self,**_args):
        _data = super().read(**_args)
        _schema = super().meta()
        for _kwargs in self._targets :
            if _schema :
                _kwargs['schema'] = _schema
            self.post(_data,**_kwargs)

        return _data
    def run(self) :
        return self.read()
    def post (self,_data,**_args) :
        """
        This function returns an instance of a process that will perform the write operation
        :_args  parameters associated with writer object
        """
        writer = transport.get.writer(**_args)
        if 'schema' in _args :
            writer.write(_data,schema=_args['schema'])
        else:
            writer.write(_data)
        writer.close()