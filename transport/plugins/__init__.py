"""
The functions within are designed to load external files and apply functions against the data
The plugins are applied as 
    - post-processing if we are reading data 
    - and pre-processing if we are writing data

The plugin will use a decorator to identify meaningful functions
@TODO: This should work in tandem with loggin (otherwise we don't have visibility into what is going on)
"""
import importlib as IL
import importlib.util
import sys
import os
import pandas as pd
import time

class Plugin :
    """
    Implementing function decorator for data-transport plugins (post-pre)-processing
    """
    def __init__(self,**_args):
        """
        :name   name of the plugin
        :mode   restrict to reader/writer
        :about  tell what the function is about    
        """
        self._name = _args['name'] if 'name' in _args else None
        self._version = _args['version'] if 'version' in _args else '0.1'
        self._doc = _args['doc'] if 'doc' in _args else "N/A"
        self._mode = _args['mode'] if 'mode' in _args else 'rw'
    def __call__(self,pointer,**kwargs):
        def wrapper(_args,**kwargs):
            return pointer(_args,**kwargs)
        #
        # @TODO:
        # add attributes to the wrapper object
        #
        self._name = pointer.__name__ if not self._name else self._name
        setattr(wrapper,'transport',True)
        setattr(wrapper,'name',self._name)
        setattr(wrapper,'version',self._version)
        setattr(wrapper,'doc',self._doc)
        return wrapper

class PluginLoader :
    """
    This class is intended to load a plugin and make it available and assess the quality of the developed plugin
    """
   
    def __init__(self,**_args):
        """
        """
        # _names = _args['names'] if 'names' in _args else None
        # path = _args['path'] if 'path' in _args else None
        # self._names = _names if type(_names) == list else [_names]
        self._modules = {}
        self._names = []
        self._registry = _args['registry']

        pass
    def load (self,**_args):
        self._modules = {}
        self._names = []
        path = _args ['path']
        if os.path.exists(path) :
            _alias = path.split(os.sep)[-1]
            spec = importlib.util.spec_from_file_location(_alias, path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module) #--loads it into sys.modules
            for _name in dir(module) :
                if self.isplugin(module,_name) :
                    self._module[_name] = getattr(module,_name)
                    # self._names [_name]
    def format (self,**_args):
        uri = _args['alias'],_args['name']
    # def set(self,_pointer) :
    def set(self,_key) :
        """
        This function will set a pointer to the list of modules to be called
        This should be used within the context of using the framework as a library
        """
        if type(_key).__name__ == 'function':
            #
            # The pointer is in the code provided by the user and loaded in memory
            #
            _pointer = _key
            _key = 'inline@'+_key.__name__
            # self._names.append(_key.__name__)
        else:
            _pointer = self._registry.get(key=_key)

        if _pointer  :
            self._modules[_key] = _pointer
            self._names.append(_key)
        
    def isplugin(self,module,name):
        """
        This function determines if a module is a recognized plugin
        :module     module object loaded from importlib
        :name       name of the functiion of interest
        """
        
        p = type(getattr(module,name)).__name__ =='function'
        q = hasattr(getattr(module,name),'transport')
        #
        # @TODO: add a generated key, and more indepth validation
        return p and q
    def has(self,_name):
        """
        This will determine if the module name is loaded or not
        """
        return _name in self._modules 
    def ratio (self):
        """
        This functiion determines how many modules loaded vs unloaded given the list of names
        """

        _n = len(self._names)
        return len(set(self._modules.keys()) & set (self._names)) / _n
    def apply(self,_data,_logger=[]):
        _input= {}
        
        for _name in self._modules :
            try:
                _input = {'action':'plugin','object':_name,'input':{'status':'PASS'}}
                _pointer = self._modules[_name]
                if type(_data) == list :
                    _data = pd.DataFrame(_data)
                _brow,_bcol = list(_data.shape) 
                
                #
                # @TODO: add exception handling
                _data = _pointer(_data)
                
                _input['input']['shape'] = {'rows-dropped':_brow - _data.shape[0]}
            except Exception as e:
                _input['input']['status'] = 'FAILED'
                print (e)
            time.sleep(1)
            if _logger:
                try:
                    _logger(**_input)
                except Exception as e:
                    pass    
        return _data
    # def apply(self,_data,_name):
    #     """
    #     This function applies an external module function against the data.
    #     The responsibility is on the plugin to properly return data, thus responsibility is offloaded
    #     """
    #     try:
            
    #         _pointer = self._modules[_name]
    #         _data = _pointer(_data)

    #     except Exception as e:
    #         pass
    #     return _data
