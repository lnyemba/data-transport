"""
This module uses callback architectural style as a writer to enable user-defined code to handle the output of a reader
The intent is to allow users to have control over the output of data to handle things like logging, encryption/decryption and other
"""
import queue
from threading import Thread, Lock
# from transport.common import Reader,Writer
import numpy as np
import pandas as pd

class Writer :
    lock = Lock()
    _queue = {'default':queue.Queue()}
    def __init__(self,**_args):
        self._cache     = {}
        self._callback  = _args['callback'] if 'callback' in _args else None
        self._id = _args['id'] if 'id' in _args else 'default'
        if self._id not in Writer._queue :
            Writer._queue[self._id] = queue.Queue()
        thread = Thread(target=self._forward)
        thread.start()
    def _forward(self):
        _q = Writer._queue[self._id]
        _data = _q.get()
        _q.task_done()       
        self._callback(_data)

    def has(self,**_args) :
        return self._callback is not None
        
   
    def close(self):
        """
        This will empty the queue and have it ready for another operation
        """
        _q = Writer._queue[self._id]
        with _q.mutex:
            _q.queue.clear()  
            _q.all_tasks_done.notify_all()  

    def write(self,_data,**_args):
        _id = _args['id'] if 'id' in _args else self._id
        
        _q = Writer._queue[_id]
        _q.put(_data)
        _q.join()


        # self.callback = print