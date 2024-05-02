"""
This class is a wrapper around read/write classes of cloud,sql,nosql,other packages
The wrapper allows for application of plugins as pre-post conditions
"""    
class IO:
    """
    Base wrapper class for read/write
    """
    def __init__(self,_agent,plugins):
        self._agent = _agent
        self._plugins = plugins
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
    def __init__(self,_agent,pipeline=None):
        super().__init__(_agent,pipeline)
    def read(self,**_args):
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
        if self._plugins and self._plugins.ratio() > 0 :
            _data = self._plugins.apply(_data)

        self._agent.write(_data,**_args)
