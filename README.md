# Introduction

This project implements an abstraction of objects that can have access to a variety of data stores, implementing read/write with a simple and expressive interface. This abstraction works with **NoSQL** and **SQL** data stores and leverages **pandas**

The supported data store providers :

| Provider | Underlying Drivers | Description |
| ---- | ---| ---- |
| sqlite| Native SQLite|SQLite3|
| postgresql| psycopg2 | PostgreSQL
| redshift| psycopg2 | Amazon Redshift
| netezza| nzpsql | IBM Neteeza
| Files: CSV, TSV| pandas| pandas data-frame
| Couchdb| cloudant | Couchbase/Couchdb
| mongodb| pymongo | Mongodb
| mysql| mysql| Mysql
| bigquery| google-bigquery| Google BigQuery
| mariadb| mysql| Mariadb
| rabbitmq|pika| RabbitMQ Publish/Subscribe

# Why Use Data-Transport ?

Mostly data scientists that don't really care about the underlying database and would like to manipulate data transparently.

1. Familiarity with **pandas data-frames**
2. Connectivity **drivers** are included
3. Useful for ETL


### Installation

Within the virtual environment perform the following command:

    pip install git+https://dev.the-phi.com/git/steve/data-transport.git

Binaries and eggs will be provided later on


### Usage

In your code, perform the 

    import transport
    from transport import factory
    #
    # importing a mongo reader
    args = {"host":"<host>:<port>","dbname":"<database>","doc":"<doc_id>",["username":"<username>","password":"<password>"]}
    reader = factory.instance(provider='mongodb',doc=<mydoc>,db=<db-name>)
    #
    # reading a document i.e just applying a find (no filters)
    #
    df    = mreader.read()  #-- pandas data frame
    df.head()

    #
    # reading from postgresql
    
    pgreader     = factory.instance(type='postgresql',database=<database>,table=<table_name>)
    pg.read()   #-- will read the table by executing a SELECT
    pg.read(sql=<sql query>)
    
    #
    # Reading a document and executing a view
    #
    document    = dreader.read()    
    result      = couchdb.view(id='<design_doc_id>',view_name=<view_name',<key=value|keys=values>)
    
