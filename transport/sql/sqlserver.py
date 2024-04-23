"""
Handling Microsoft SQL Server via pymssql driver/connector
"""
import sqlalchemy
import pandas as pd
from transport.sql.common import Base, BaseReader, BaseWriter


class MsSQLServer:
    def __init__(self,**_args) :
        super().__init__(**_args)
        pass
    def get_provider(self):
        # mssql+pymssql://scott:tiger@hostname:port/dbname"
        return "mssql+pymssql"
    def get_default_port(self):
        return "1433"
class Reader (MsSQLServer,BaseReader):
    def __init__(self,**_args):
        super().__init__(**_args)
        
class Writer (MsSQLServer,BaseWriter):
    def __init__(self,**_args):
        super().__init__(**_args)