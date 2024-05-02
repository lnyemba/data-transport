"""
We are implementing transport to and from nextcloud (just like s3)
"""
import os
import sys
from transport.common import IEncoder
import pandas as pd
from io import StringIO
import json
import nextcloud_client as nextcloud

class Nextcloud :
    def __init__(self,**_args):
        pass
        self._delimiter = None
        self._handler = nextcloud.Client(_args['url'])
        _uid = _args['uid']
        _token = _args['token']
        self._uri = _args['folder'] if 'folder' in _args else './'
        if self._uri.endswith('/') :
            self._uri = self._uri[:-1]
        self._file = None if 'file' not in _args else _args['file']
        self._handler.login(_uid,_token)
    def close(self):
        try:
            self._handler.logout()
        except Exception as e:
            pass
        
    
class Reader(Nextcloud):
    def __init__(self,**_args):
        # self._file = [] if 'file' not in _args else _args['file']
        super().__init__(**_args)
        pass
    def read(self,**_args):
        _filename = self._file if 'file' not in _args else _args['file']
        #
        # @TODO: if _filename is none, an exception should be raised
        #
        _uri = '/'.join([self._uri,_filename])
        if self._handler.get_file(_uri) :
            #
            #
            _info = self._handler.file_info(_uri)
            _content = self._handler.get_file_contents(_uri).decode('utf8')
            if _info.get_content_type() == 'text/csv' :
                #
                # @TODO: enable handling of csv, xls, parquet, pickles
                _file = StringIO(_content)
                return pd.read_csv(_file)
            else:
                #
                # if it is neither a structured document like csv, we will return the content as is
                return _content
        return None     
class Writer (Nextcloud):
    """
    This class will write data to an instance of nextcloud
    """
    def __init__(self,**_args)    :
        super().__init__(**_args)
        self
    def write(self,_data,**_args):
        """
        This function will upload a file to a given destination 
        :file   has the uri of the location of the file
        """
        _filename = self._file if 'file' not in _args else _args['file']
        _uri = '/'.join([self._uri,_filename])
        if type(_data) == pd.DataFrame :
            f = StringIO()
            _data.to_csv(f,index=False)
            _content = f.getvalue()
        elif type(_data) == dict :
            _content = json.dumps(_data,cls=IEncoder)
        else:
            _content = str(_data)
        self._handler.put_file_contents(_uri,_content)

