"""
This class is a wrapper around read/write classes of cloud,sql,nosql,other packages
The wrapper allows for application of plugins as pre-post conditions.
NOTE: Plugins are converted to a pipeline, so we apply a pipeline when reading or writing:
        - upon initialization we will load plugins
        - on read/write we apply a pipeline (if passed as an argument)
"""    
from transport.plugins import plugin, PluginLoader
import transport
from transport import providers
from multiprocessing import Process
import time


class IO:
    """
    Base wrapper class for read/write and support for logs
    """
    def __init__(self,_agent,plugins):
        self._agent = _agent
        if plugins :
            self._init_plugins(plugins)
        else:
            self._plugins = None
        
    def _init_plugins(self,_args):
        """
        This function will load pipelined functions as a plugin loader
        """
        if 'path' in _args and 'names' in _args :
            self._plugins = PluginLoader(**_args)
        else:
            self._plugins = PluginLoader()
            [self._plugins.set(_pointer) for _pointer in _args]
        #
        # @TODO: We should have a way to log what plugins are loaded and ready to use
    def meta (self,**_args):
        if hasattr(self._agent,'meta') :
            return self._agent.meta(**_args)
        return []

    def close(self):
        if hasattr(self._agent,'close') :
            self._agent.close()
    def apply(self):
        """
        applying pre/post conditions given a pipeline expression
        """
        for _pointer in self._plugins :
            _data = _pointer(_data)
    def apply(self,_query):
        if hasattr(self._agent,'apply') :
            return self._agent.apply(_query)
        return None
class IReader(IO):
    """
    This is a wrapper for read functionalities
    """
    def __init__(self,_agent,pipeline=None):
        super().__init__(_agent,pipeline)
    def read(self,**_args):
        if 'pipeline' in _args :
            self._init_plugins(_args['pipeline'])
        _data = self._agent.read(**_args)
        if self._plugins and self._plugins.ratio() > 0 :
            _data = self._plugins.apply(_data)
        #
        # output data 
        return _data
class IWriter(IO):
    def __init__(self,_agent,pipeline=None):
        super().__init__(_agent,pipeline)  
    def write(self,_data,**_args):
        if 'pipeline' in _args :
            self._init_plugins(_args['pipeline'])
        if self._plugins and self._plugins.ratio() > 0 :
            _data = self._plugins.apply(_data)

        self._agent.write(_data,**_args)

#
# The ETL object in its simplest form is an aggregation of read/write objects
# @TODO: ETL can/should aggregate a writer as a plugin and apply it as a process

def _ProcessWriter (_data,_args):
    writer = transport.get.writer(**_args)
    writer.write(_data)

class IETL(IReader) :
    """
    This class performs an ETL operation by ineriting a read and adding writes as pipeline functions
    """
    def __init__(self,**_args):
        super().__init__(transport.get.reader(**_args['source']))
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

        for _kwargs in self._targets :
            self.post(_data,**_kwargs)
        #     pthread = Process(target=_ProcessWriter,args=(_data,_kwargs))
        #     pthread.start()
        #     self.jobs.append(pthread)
            
        # if not self._hasParentProcess :
        #     while self.jobs :
        #         jobs = [pthread for pthread in self.jobs if pthread.is_alive()]
        #         time.sleep(1)
    
        return _data
    def post (self,_data,**_args) :
        """
        This function returns an instance of a process that will perform the write operation
        :_args  parameters associated with writer object
        """
        writer = transport.get.writer(**_args)
        writer.write(_data)
        writer.close()