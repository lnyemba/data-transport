import os
import json
from info import __version__
import copy
import transport
import importlib
import importlib.util
import shutil
from io import StringIO

"""
This class manages data from the registry and allows (read only)
@TODO: add property to the DATA attribute
"""
if 'HOME' in os.environ :
    REGISTRY_PATH=os.sep.join([os.environ['HOME'],'.data-transport'])
else:
    REGISTRY_PATH=os.sep.join([os.environ['USERPROFILE'],'.data-transport'])

#
# This path can be overriden by an environment variable ...
#
if 'DATA_TRANSPORT_REGISTRY_PATH' in os.environ :
    REGISTRY_PATH = os.environ['DATA_TRANSPORT_REGISTRY_PATH']
REGISTRY_FILE= 'transport-registry.json'
DATA = {}


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
    if os.path.exists(path) and os.path.exists(reg_file):
        if type(auth_file) == str and os.path.exists (auth_file) :
            f = open(auth_file)
        elif type(auth_file) == StringIO:
            f = auth_file
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

