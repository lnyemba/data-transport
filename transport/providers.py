# from transport.common import Reader, Writer,Console #, factory
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
from transport import session
from transport import nextcloud
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
MYSQL = 'mysql+mysqlconnector'
RABBITMQ = 'rabbitmq'
MARIADB  = 'mariadb'
COUCHDB = 'couch'
CONSOLE = 'console'
ETL = 'etl'
NEXTCLOUD = 'nextcloud'

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
QUEUE = QLISTENER
DATABRICKS= 'databricks+connector'
DRIVERS  = {PG:pg,REDSHIFT:pg,MYSQL:my,MARIADB:my,NETEZZA:nz,SQLITE:sqlite3}
CATEGORIES ={'sql':[NETEZZA,PG,MYSQL,REDSHIFT,SQLITE,MARIADB],'nosql':[MONGODB,COUCHDB],'cloud':[NEXTCLOUD,S3,BIGQUERY,DATABRICKS],'file':[FILE],
             'queue':[RABBIT,QLISTENER],'memory':[CONSOLE,QUEUE],'http':[HTTP]}

READ = {'sql':sql.SQLReader,'nosql':{MONGODB:mongo.MongoReader,COUCHDB:couch.CouchReader},
        'cloud':{BIGQUERY:sql.BigQueryReader,DATABRICKS:bricks.BricksReader,NEXTCLOUD:nextcloud.NextcloudReader},
        'file':disk.DiskReader,'queue':{RABBIT:queue.QueueReader,QLISTENER:qlistener.qListener},
        # 'cli':{CONSOLE:Console},'memory':{CONSOLE:Console},'http':session.HttpReader
        }
WRITE = {'sql':sql.SQLWriter,'nosql':{MONGODB:mongo.MongoWriter,COUCHDB:couch.CouchWriter},
         'cloud':{BIGQUERY:sql.BigQueryWriter,DATABRICKS:bricks.BricksWriter,NEXTCLOUD:nextcloud.NextcloudWriter},
         'file':disk.DiskWriter,'queue':{RABBIT:queue.QueueWriter,QLISTENER:qlistener.qListener},
        #  'cli':{CONSOLE:Console},
        #  'memory':{CONSOLE:Console}, 'http':session.HttpReader
         
        }
# SQL_PROVIDERS = [POSTGRESQL,MYSQL,NETEZZA,MARIADB,SQLITE]
PROVIDERS = {
    FILE:{'read':disk.DiskReader,'write':disk.DiskWriter},
    SQLITE:{'read':disk.SQLiteReader,'write':disk.SQLiteWriter,'driver':sqlite3},
    
    POSTGRESQL:{'read':sql.SQLReader,'write':sql.SQLWriter,'driver':pg,'default':{'host':'localhost','port':5432}},
    NETEZZA:{'read':sql.SQLReader,'write':sql.SQLWriter,'driver':nz,'default':{'port':5480}},
    REDSHIFT:{'read':sql.SQLReader,'write':sql.SQLWriter,'driver':pg,'default':{'host':'localhost','port':5432}},
    RABBITMQ:{'read':queue.QueueReader,'writer':queue.QueueWriter,'context':queue.QueueListener,'default':{'host':'localhost','port':5432}},
    
    MYSQL:{'read':sql.SQLReader,'write':sql.SQLWriter,'driver':my,'default':{'host':'localhost','port':3306}},
    MARIADB:{'read':sql.SQLReader,'write':sql.SQLWriter,'driver':my,'default':{'host':'localhost','port':3306}},
    
    S3:{'read':s3.s3Reader,'write':s3.s3Writer},
    BIGQUERY:{'read':sql.BigQueryReader,'write':sql.BigQueryWriter},
    DATABRICKS:{'read':bricks.BricksReader,'write':bricks.BricksWriter},
    NEXTCLOUD:{'read':nextcloud.NextcloudReader,'write':nextcloud.NextcloudWriter},

    QLISTENER:{'read':qlistener.qListener,'write':qlistener.qListener,'default':{'host':'localhost','port':5672}},
    CONSOLE:{'read':qlistener.Console,"write":qlistener.Console},
    HTTP:{'read':session.HttpReader,'write':session.HttpWriter},
    
    MONGODB:{'read':mongo.MongoReader,'write':mongo.MongoWriter,'default':{'port':27017,'host':'localhost'}},
    COUCHDB:{'read':couch.CouchReader,'writer':couch.CouchWriter,'default':{'host':'localhost','port':5984}},
    ETL :{'read':etl.Transporter,'write':etl.Transporter}
}
DEFAULT = {PG:{'host':'localhost','port':5432},MYSQL:{'host':'localhost','port':3306}}
DEFAULT[MONGODB] = {'port':27017,'host':'localhost'}
DEFAULT[REDSHIFT] = DEFAULT[PG]
DEFAULT[MARIADB] = DEFAULT[MYSQL]
DEFAULT[NETEZZA] = {'port':5480}