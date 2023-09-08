from transport.common import Reader, Writer,Console #, factory
from transport import disk
import sqlite3
from transport import s3 as s3
from transport import rabbitmq as queue
from transport import couch as couch
from transport import mongo as mongo
from transport import sql as sql
from transport import etl as etl
from transport import qlistener
from transport import bricks
import psycopg2 as pg
import mysql.connector as my
from google.cloud import bigquery as bq
import nzpy as nz   #--- netezza drivers
import os

from transport.version import __version__

POSTGRESQL 	= 'postgresql'
MONGODB 	= 'mongodb'
HTTP='http'
BIGQUERY	='bigquery'
FILE 	= 'file'
ETL = 'etl'
SQLITE = 'sqlite'
SQLITE3= 'sqlite'
REDSHIFT = 'redshift'
NETEZZA = 'netezza'
MYSQL = 'mysql'
RABBITMQ = 'rabbitmq'
MARIADB  = 'mariadb'
COUCHDB = 'couch'
CONSOLE = 'console'
ETL = 'etl'
#
# synonyms of the above
BQ 		= BIGQUERY
MONGO 	= MONGODB
FERRETDB= MONGODB
PG 		= POSTGRESQL
PSQL 	= POSTGRESQL
PGSQL	= POSTGRESQL
S3      = 's3'	
AWS_S3  = 's3'
RABBIT = RABBITMQ

QLISTENER = 'qlistener'
DATABRICKS= 'databricks+connector'
DRIVERS  = {PG:pg,REDSHIFT:pg,MYSQL:my,MARIADB:my,NETEZZA:nz,SQLITE:sqlite3}
CATEGORIES ={'sql':[NETEZZA,PG,MYSQL,REDSHIFT,SQLITE,MARIADB],'nosql':[MONGODB,COUCHDB],'cloud':[BIGQUERY,DATABRICKS],'file':[FILE],
             'queue':[RABBIT,QLISTENER],'memory':[CONSOLE,QLISTENER],'http':[HTTP]}

READ = {'sql':sql.SQLReader,'nosql':{MONGODB:mongo.MongoReader,COUCHDB:couch.CouchReader},
        'cloud':{BIGQUERY:sql.BigQueryReader,DATABRICKS:bricks.BricksReader},
        'file':disk.DiskReader,'queue':{RABBIT:queue.QueueReader,QLISTENER:qlistener.qListener},
        'cli':{CONSOLE:Console},'memory':{CONSOLE:Console}
        }
WRITE = {'sql':sql.SQLWriter,'nosql':{MONGODB:mongo.MongoWriter,COUCHDB:couch.CouchWriter},
         'cloud':{BIGQUERY:sql.BigQueryWriter,DATABRICKS:bricks.BricksWriter},
         'file':disk.DiskWriter,'queue':{RABBIT:queue.QueueWriter,QLISTENER:qlistener.qListener},'cli':{CONSOLE:Console},'memory':{CONSOLE:Console}
         
        }
DEFAULT = {PG:{'host':'localhost','port':5432},MYSQL:{'host':'localhost','port':3306}}
DEFAULT[MONGODB] = {'port':27017,'host':'localhost'}
DEFAULT[REDSHIFT] = DEFAULT[PG]
DEFAULT[MARIADB] = DEFAULT[MYSQL]
DEFAULT[NETEZZA] = {'port':5480}