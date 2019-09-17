"""
This is a build file for the 
"""
from setuptools import setup, find_packages
 
setup(
    name = "data-transport",
    version = "1.0",
    author = "The Phi Technology LLC",
    author_email = "steve@the-phi.com",
    license = "MIT",
    packages=['transport'],
    install_requires = ['pymongo','numpy','cloudant','pika','boto','flask-session','smart_open'],

    use_2to3=True,
    convert_2to3_doctests=['src/your/module/README.txt'],
    use_2to3_fixers=['your.fixers'],
    use_2to3_exclude_fixers=['lib2to3.fixes.fix_import'],
    )
