"""
This is a build file for the 
"""
from setuptools import setup, find_packages
import os
import sys
# from version import __version__,__author__
from info import __version__, __author__


# __author__ = 'The Phi Technology'
# __version__= '1.8.0'

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read() 
args    = {
    "name":"data-transport",
    "version":__version__,
    "author":__author__,"author_email":"info@the-phi.com",
    "license":"MIT",
    # "packages":["transport","info","transport/sql"]},

    "packages": find_packages(include=['info', 'transport.*'])}
args["keywords"]=['mongodb','couchdb','rabbitmq','file','read','write','s3','sqlite']
args["install_requires"] = ['pyncclient','pymongo','sqlalchemy','pandas','typer','pandas-gbq','numpy','cloudant','pika','nzpy','boto3','boto','pyarrow','google-cloud-bigquery','google-cloud-bigquery-storage','flask-session','smart_open','botocore','psycopg2-binary','mysql-connector-python','numpy']
args["url"] =   "https://healthcareio.the-phi.com/git/code/transport.git"
args['scripts'] = ['bin/transport']
if sys.version_info[0] == 2 :
    args['use_2to3'] = True
    args['use_2to3_exclude_fixers']=['lib2to3.fixes.fix_import']
setup(**args)
