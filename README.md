# Introduction

This project implements an abstraction of objects that can have access to a variety of data stores, implementing read/write functions associated and specific to the data-sources. The classes implement functionalities against :

    - Rabbitmq-server
    - Couchdb-server
    - Http Session : {csv,tab,pipe,sql}
    - Disk{Reader|Writer} : csv, tab, pipe, sql on disk


### Usage

The basic usage revolves around a factory class (to be a singleton)

    import transport
    
    p = {"uri":"https://your-server:5984","dbname":"mydatabase","doc":"doc_id"}
    couchdb = transport.Factory.instance(type='CouchdbReader',args=p)
    
    #
    # let's execute a view
    #
    result = couchdb.view('view_name/function',key=value)
    info   = couchdb.read()