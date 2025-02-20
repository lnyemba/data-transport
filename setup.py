"""
This is a build file for the 
"""
from setuptools import setup, find_packages
import os
import sys
# from version import __version__,__author__
from info import __version__, __author__,__app_name__,__license__,__edition__


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read() 
args    = {
    "name":__app_name__,
    "version":__version__,
    "author":__author__,"author_email":"info@the-phi.com",
    "license":__license__,
    # "packages":["transport","info","transport/sql"]},

    "packages": find_packages(include=['info','transport', 'transport.*'])}
args["keywords"]=['mongodb','duckdb','couchdb','rabbitmq','file','read','write','s3','sqlite']
args["install_requires"] = ['pyncclient','duckdb-engine','pymongo','sqlalchemy','pandas','typer','pandas-gbq','numpy','cloudant','pika','nzpy','termcolor','boto3','boto','pyarrow','google-cloud-bigquery','google-cloud-bigquery-storage','flask-session','smart_open','botocore','psycopg2-binary','mysql-connector-python','numpy','pymssql','pyspark','pydrill','sqlalchemy_drill']
args["url"] =   "https://healthcareio.the-phi.com/git/code/transport.git"
args['scripts'] = ['bin/transport']
args['classifiers'] = ['Programming Language :: Python :: 3',
                    'License :: OSI Approved :: MIT License',
                     'Operating System :: OS Independent',
                                ],
# if sys.version_info[0] == 2 :
#     args['use_2to3'] = True
#     args['use_2to3_exclude_fixers']=['lib2to3.fixes.fix_import']
setup(**args)
