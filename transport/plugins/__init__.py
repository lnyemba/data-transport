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

class plugin :
    """
    Implementing function decorator for data-transport plugins (post-pre)-processing
    """
    def __init__(self,**_args):
        """
        :name   name of the plugin
        :mode   restrict to reader/writer
        :about  tell what the function is about    
        """
        self._name = _args['name']
        self._about = _args['about']
        self._mode = _args['mode'] if 'mode' in _args else 'rw'
    def __call__(self,pointer,**kwargs):
        def wrapper(_args,**kwargs):
            return pointer(_args,**kwargs)
        #
        # @TODO:
        # add attributes to the wrapper object
        #
        setattr(wrapper,'transport',True)
        setattr(wrapper,'name',self._name)
        setattr(wrapper,'mode',self._mode)
        setattr(wrapper,'about',self._about)
        return wrapper


class PluginLoader :
    """
    This class is intended to load a plugin and make it available and assess the quality of the developed plugin
    """
    def __init__(self,**_args):
        """
        :path   location of the plugin (should be a single file)
        :_names of functions to load
        """
        _names = _args['names'] if 'names' in _args else None
        path = _args['path'] if 'path' in _args else None
        self._names = _names if type(_names) == list else [_names]
        self._modules = {}
        self._names = []
        if path and os.path.exists(path) and _names:
            for _name in self._names :
                
                spec = importlib.util.spec_from_file_location('private', path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module) #--loads it into sys.modules
                if hasattr(module,_name) :
                    if self.isplugin(module,_name) :
                        self._modules[_name] = getattr(module,_name)
                    else:
                        print ([f'Found {_name}', 'not plugin'])
                else:
                    #
                    # @TODO: We should log this somewhere some how
                    print (['skipping ',_name, hasattr(module,_name)])
                    pass
        else:
            #
            # Initialization is empty
            self._names = []
        pass
    def set(self,_pointer) :
        """
        This function will set a pointer to the list of modules to be called
        This should be used within the context of using the framework as a library
        """
        _name = _pointer.__name__
        
        self._modules[_name] = _pointer
        self._names.append(_name)
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
    def apply(self,_data):
        for _name in self._modules :
            _pointer = self._modules[_name]
            #
            # @TODO: add exception handling
            _data = _pointer(_data)
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
