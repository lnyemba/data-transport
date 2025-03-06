import os
import json
from info import __version__
import copy
import transport
import importlib
import importlib.util
import shutil


"""
This class manages data from the registry and allows (read only)
@TODO: add property to the DATA attribute
"""

REGISTRY_PATH=os.sep.join([os.environ['HOME'],'.data-transport'])
#
# This path can be overriden by an environment variable ...
#
if 'DATA_TRANSPORT_REGISTRY_PATH' in os.environ :
    REGISTRY_PATH = os.environ['DATA_TRANSPORT_REGISTRY_PATH']
REGISTRY_FILE= 'transport-registry.json'
DATA = {}
# class plugins:
#     #
#     # This is a utility function that should enable management of plugins-registry
#     # The class allows to add/remove elements 
#     #
#     # @TODO: add read/write properties to the class (better design practice)
#     #
#     _data = {}
#     FOLDER = os.sep.join([REGISTRY_PATH,'plugins'])
#     CODE = os.sep.join([REGISTRY_PATH,'plugins','code'])
#     FILE = os.sep.join([REGISTRY_PATH,'plugin-registry.json'])
#     @staticmethod
#     def init():
        
#         if not os.path.exists(plugins.FOLDER) :
#             os.makedirs(plugins.FOLDER)
#         if not os.path.exists(plugins.CODE):
#             os.makedirs(plugins.CODE)
#         if not os.path.exists(plugins.FILE):
#             f = open(plugins.FILE,'w')
#             f.write("{}")
#             f.close()
#         plugins._read() #-- will load data as a side effect

#     @staticmethod
#     def copy (path) :
        
#         shutil.copy2(path,plugins.CODE)
#     @staticmethod
#     def _read ():
#         f = open(plugins.FILE)
#         try:
#             _data = json.loads(f.read())
#             f.close()
#         except Exception as e:
#             print (f"Corrupted registry, resetting ...")
#             _data = {}
#             plugins._write(_data)
            
#         plugins._data = _data
#     @staticmethod
#     def _write (_data):
#         f = open(plugins.FILE,'w')
#         f.write(json.dumps(_data))
#         f.close()
#         plugins._data = _data

#     @staticmethod
#     def inspect (_path):
#         _names = []
        
#         if os.path.exists(_path) :
#             _filename = _path.split(os.sep)[-1]
#             spec = importlib.util.spec_from_file_location(_filename, _path)
#             module = importlib.util.module_from_spec(spec)
#             spec.loader.exec_module(module)

#             # _names = [{'name':getattr(getattr(module,_name),'name'),'pointer':getattr(module,_name)} for _name in dir(module) if type( getattr(module,_name)).__name__ == 'function']
#             for _name in dir(module) :
#                 _pointer = getattr(module,_name) 
#                 if hasattr(_pointer,'transport') :
#                     _item = {'real_name':_name,'name':getattr(_pointer,'name'),'pointer':_pointer,'version':getattr(_pointer,'version')}
#                     _names.append(_item)

            
#         return _names
#     @staticmethod
#     def add (alias,path):
#         """
#         Add overwrite the registry entries
#         """
#         _names = plugins.inspect (path)
#         _log = []
        
#         if _names :
#             #
#             # We should make sure we have all the plugins with the attributes (transport,name) set
#             _names = [_item for _item in _names if hasattr(_item['pointer'],'transport') ]
#             if _names :
#                 plugins.copy(path)
#                 _content = []
                
#                 for _item in _names :
#                     _key = '@'.join([alias,_item['name']])
#                     _log.append(_item['name'])
#                 #
#                 # Let us update the registry 
#                 # 
#                 plugins.update(alias,path,_log)        
#         return _log
    
#     @staticmethod
#     def update (alias,path,_log) :
#         """
#         updating the registry entries of the plugins (management data)
#         """
#         # f = open(plugins.FILE)
#         # _data = json.loads(f.read())
#         # f.close()
#         _data = plugins._data
#         # _log = plugins.add(alias,path)
        
#         if _log :
#             _data[alias] = {'content':_log,'name':path.split(os.sep)[-1]}
#             plugins._write(_data) #-- will update data as a side effect

#         return _log
#     @staticmethod
#     def get(**_args) :
#         # f = open(plugins.FILE)
#         # _data = json.loads(f.read())
#         # f.close()
#         # if 'key' in _args :
#         #     alias,name = _args['key'].split('.') if '.' in _args['key'] else _args['key'].split('@')
#         # else :
#         #     alias = _args['alias']
#         #     name  = _args['name']
        
#         # if alias in _data :
            
#         #     _path = os.sep.join([plugins.CODE,_data[alias]['name']])
#         #     _item = [_item for _item in plugins.inspect(_path) if name == _item['name']]
            
#         #     _item = _item[0] if _item else None
#         #     if _item :
                
#         #         return _item['pointer'] 
#         # return None
#         _item = plugins.has(**_args)
#         return _item['pointer'] if _item else None
    
#     @staticmethod
#     def has (**_args):
#         f = open(plugins.FILE)
#         _data = json.loads(f.read())
#         f.close()
#         if 'key' in _args :
#             alias,name = _args['key'].split('.') if '.' in _args['key'] else _args['key'].split('@')
#         else :
#             alias = _args['alias']
#             name  = _args['name']
        
#         if alias in _data :
            
#             _path = os.sep.join([plugins.CODE,_data[alias]['name']])
#             _item = [_item for _item in plugins.inspect(_path) if name == _item['name']]
            
#             _item = _item[0] if _item else None
#             if _item :
                
#                 return copy.copy(_item)
#         return None
#     @staticmethod
#     def synch():
#         pass

def isloaded ():
    return DATA not in [{},None]
def exists (path=REGISTRY_PATH,_file=REGISTRY_FILE) :
    """
    This function determines if there is a registry at all
    """
    p = os.path.exists(path)
    q = os.path.exists( os.sep.join([path,_file]))
    
    return p and q
def load (_path=REGISTRY_PATH,_file=REGISTRY_FILE):
    global DATA
    
    if exists(_path) :
        path = os.sep.join([_path,_file])
        f = open(path)
        DATA = json.loads(f.read())
        f.close()
def init (email,path=REGISTRY_PATH,override=False,_file=REGISTRY_FILE):
    """
    Initializing the registry and will raise an exception in the advent of an issue
    """
    p = '@' in email
    q = False if '.' not in email else email.split('.')[-1] in ['edu','com','io','ai','org']
    if p and q :
        _config = {"email":email,'version':__version__}
        if not os.path.exists(path):
            os.makedirs(path)
        filename = os.sep.join([path,_file])
        if not os.path.exists(filename) or override == True :

            f = open(filename,'w')
            f.write( json.dumps(_config))
            f.close()
            # _msg = f"""{CHECK_MARK} Successfully wrote configuration to {path} from {email}"""
            
        else:
            raise Exception (f"""Unable to write configuration, Please check parameters (or help) and try again""")
    else:
        raise Exception (f"""Invalid Input, {email} is not well formatted, provide an email with adequate format""")
def lookup (label):
    global DATA
    return label in DATA
has = lookup 

def get (label='default') :
    global DATA
    return copy.copy(DATA[label]) if label in DATA else {}

def set (label, auth_file, default=False,path=REGISTRY_PATH) :
    """
    This function will add a label (auth-file data) into the registry and can set it as the default
    """
    if label == 'default' :
        raise Exception ("""Invalid label name provided, please change the label name and use the switch""")
    reg_file = os.sep.join([path,REGISTRY_FILE])
    if os.path.exists (auth_file) and os.path.exists(path) and os.path.exists(reg_file):
        f = open(auth_file)
        _info = json.loads(f.read())
        f.close()
        f = open(reg_file)
        _config = json.loads(f.read())
        f.close()

        #
        # set the proposed label
        _object = transport.factory.instance(**_info)
        if _object :
            _config[label] = _info
            if default :
                _config['default'] = _info
            #
            # now we need to write this to the location
            f = open(reg_file,'w')
            f.write(json.dumps(_config))
            f.close()
        else:
            raise Exception( f"""Unable to load file locate at {path},\nLearn how to generate auth-file with wizard found at https://healthcareio.the-phi.com/data-transport""")
        pass         
    else:
        pass
    pass

