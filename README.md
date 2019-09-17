# Introduction

This project implements an abstraction of objects that can have access to a variety of data stores, implementing read/write with a simple interface against specific various data-sources. The supported data sources implement functionalities against :

    - Rabbitmq-server
    - Couchdb-server
    - Mongodb-server
    - Http Session : {csv,tab,pipe,sql}
    - Disk{Reader|Writer} : csv, tab, pipe, sql on disk

Such an interface is used to facilitate data transport in and out of a store for whatever an application may need (log, session management, ...)

### Installation

Within the virtual environment perform the following command:

    pip install git+https://dev.the-phi.com/git/steve/data-transport.git

Binaries and eggs will be provided later on


### Usage

The basic usage revolves around a factory class (to be a singleton)

    import transport
    from transport import factory
    #
    # importing a mongo reader
    args = {"host":"<host>:<port>","dbname":"<database>","doc":"<doc_id>",["username":"<username>","password":"<password>"]}
    mreader = factory.instance(type='mongo.MonoReader',args=args)
    #
    # reading a document and executing a view
    #
    document    = mreader.read()
    result      = mreader.view(name)
    #
    # importing a couchdb reader
    args = {"url":"<http://host>:<port>","dbname":"<database>","doc":"<doc_id>","username":"<username>","password":"<password>"}
    creader     = factory.instance(type='couch.CouchReader',args=args)
    
    #
    # Reading a document and executing a view
    #
    document    = dreader.read()    
    result      = couchdb.view(id='<design_doc_id>',view_name=<view_name',<key=value|keys=values>)
    
