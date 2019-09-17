"""
This is a build file for the 
"""
from setuptools import setup, find_packages
import os
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read() 
setup(
    name = "data-transport",
    version = "1.0",
    author = "The Phi Technology LLC",
    author_email = "steve@the-phi.com",
    license = "MIT",
    packages=['transport'],
    keywords=['mongodb','couchdb','rabbitmq','file','read','write','s3'],
    install_requires = ['pymongo','numpy','cloudant','pika','boto','flask-session','smart_open'],
    url="https://dev.the-phi.com/git/steve/data-transport.git",
    use_2to3=True,
    long_description=read('README.md'),
    convert_2to3_doctests=['README.md'],
    use_2to3_fixers=['your.fixers'],
    use_2to3_exclude_fixers=['lib2to3.fixes.fix_import'],
    )
