"""
This file implements support for mysql and maria db (with drivers mysql+mysql)
"""
from transport.sql.common import BaseReader, BaseWriter
# import mysql.connector as my
class MYSQL:
    
    def get_provider(self):
        return "mysql+mysqlconnector"
    def get_default_port(self):
        return "3306"
class Reader(MYSQL,BaseReader) :
    def __init__(self,**_args):
        super().__init__(**_args)

class Writer(MYSQL,BaseWriter) :
    def __init__(self,**_args):
        super().__init__(**_args)