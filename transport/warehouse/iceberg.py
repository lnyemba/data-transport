"""
dependency:
    - spark and SPARK_HOME environment variable must be set
NOTE:
    When using streaming option, insure that it is inline with default (1000 rows) or increase it in spark-defaults.conf

"""
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import col, to_date, to_timestamp
import copy

class Iceberg :
    def __init__(self,**_args):
        """
        providing catalog meta information (you must get this from apache iceberg)
        """
        #
        # Turning off logging (it's annoying & un-professional)
        #
        # _spconf = SparkContext()
        # _spconf.setLogLevel("ERROR")
        #
        # @TODO:
        #   Make arrangements for additional configuration elements 
        #
        self._session = SparkSession.builder.appName("data-transport").getOrCreate()
        self._session.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        # self._session.sparkContext.setLogLevel("ERROR")
        self._catalog = self._session.catalog
        self._table = _args['table'] if 'table' in _args else None
        
        if 'catalog' in _args :
            #
            # Let us set the default catalog
            self._catalog.setCurrentCatalog(_args['catalog'])
            
        else:
            # No current catalog has been set ...
            pass
        if 'database' in _args :
            self._database = _args['database']
            self._catalog.setCurrentDatabase(self._database)
        else:
            #
            # Should we set the default as the first one if available ?
            #
            pass
        self._catalogName = self._catalog.currentCatalog()
        self._databaseName = self._catalog.currentDatabase()
    def meta (self,**_args) :
        """
        This function should return the schema of a table (only)
        """
        _schema = []
        try:
            _table = _args['table'] if 'table' in _args else self._table
            _tableName = self._getPrefix(**_args) + f".{_table}"
            _tmp = self._session.table(_tableName).schema
            _schema = _tmp.jsonValue()['fields']
            for _item in _schema :
                del _item['nullable'],_item['metadata']
        except Exception as e:
            
            pass
        return _schema
    def _getPrefix (self,**_args):        
        _catName = self._catalogName if 'catalog' not in _args else _args['catalog']
        _datName = self._databaseName if 'database' not in _args else _args['database']
        
        return '.'.join([_catName,_datName])
    def apply(self,_query):
        """
        sql query/command to run against apache iceberg
        """
        return self._session.sql(_query).toPandas()
    def has (self,**_args):
        try:
            _prefix = self._getPrefix(**_args)
            if _prefix.endswith('.') :
                return False
            return _args['table'] in [_item.name for _item in self._catalog.listTables(_prefix)]
        except Exception as e:
            print (e)
            return False
    
    def close(self):
        self._session.stop()
class Reader(Iceberg) :
    def __init__(self,**_args):
        super().__init__(**_args)
    def read(self,**_args):
        _table = self._table
        _prefix = self._getPrefix(**_args)        
        if 'table' in _args or _table:
            _table = _args['table'] if 'table' in _args else _table
            _table = _prefix + f'.{_table}'
            return self._session.table(_table).toPandas()
        else:
            sql = _args['sql']
            return self._session.sql(sql).toPandas()
        pass
class Writer (Iceberg):
    """
    Writing data to an Apache Iceberg data warehouse (using pyspark)
    """
    def __init__(self,**_args):
        super().__init__(**_args)
        self._mode = 'append' if 'mode' not in _args else _args['mode']
        self._table = None if 'table' not in _args else _args['table']
    def format (self,_schema) :
        _iceSchema = StructType([])
        _map = {'integer':IntegerType(),'float':DoubleType(),'double':DoubleType(),'date':DateType(),
                'timestamp':TimestampType(),'datetime':TimestampType(),'string':StringType(),'varchar':StringType()}
        for _item in _schema :
            _name = _item['name']
            _type = _item['type'].lower()
            if _type not in _map :
                _iceType = StringType()
            else:
                _iceType = _map[_type]
            
            _iceSchema.add (StructField(_name,_iceType,True))
        return _iceSchema if len(_iceSchema) else []
    def write(self,_data,**_args):
        _prefix = self._getPrefix(**_args)
        if 'table' not in _args and not self._table :
            raise Exception (f"Table Name should be specified for catalog/database {_prefix}")
        _schema = self.format(_args['schema']) if 'schema' in _args else []
        if not _schema :
            rdd = self._session.createDataFrame(_data,verifySchema=False)
        else :
            rdd = self._session.createDataFrame(_data,schema=_schema,verifySchema=True)
        _mode = self._mode if 'mode' not in _args else _args['mode']
        _table = self._table if 'table' not in _args else _args['table']
        
        # print (_data.shape,_mode,_table)
        
        if not self._session.catalog.tableExists(_table):
        #     # @TODO:
        #     # add partitioning information here 
            rdd.writeTo(_table).using('iceberg').create()
            
        # #     _mode = 'overwrite'
        # #     rdd.write.format('iceberg').mode(_mode).saveAsTable(_table)
        else:
            # rdd.writeTo(_table).append()
        # #     _table = f'{_prefix}.{_table}'

            rdd.coalesce(10).write.format('iceberg').mode('append').save(_table)
