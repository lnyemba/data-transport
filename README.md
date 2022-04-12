# Introduction

This project implements an abstraction of objects that can have access to a variety of data stores, implementing read/write with a simple and expressive interface. This abstraction works with **NoSQL** and **SQL** data stores and leverages **pandas**.

The supported data store providers :

| Provider | Underlying Drivers | Description |
| :---- | :----: | ----: |
| sqlite| Native SQLite|SQLite3|
| postgresql| psycopg2 | PostgreSQL
| redshift| psycopg2 | Amazon Redshift
| s3| boto3 | Amazon Simple Storage Service
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
3. Useful for data migrations or ETL

# Usage

## Installation

Within the virtual environment perform the following :

    pip install git+https://dev.the-phi.com/git/steve/data-transport.git

Once installed **data-transport** can be used as a library in code or a command line interface (CLI)

## Data Transport as a Library (in code)
---

The data-transport can be used within code as a library 
* Read/Write against [mongodb](https://github.com/lnyemba/data-transport/wiki/mongodb)
* Read/Write against tranditional [RDBMS](https://github.com/lnyemba/data-transport/wiki/rdbms)
* Read/Write against [bigquery](https://github.com/lnyemba/data-transport/wiki/bigquery)
* ETL CLI/Code [ETL](https://github.com/lnyemba/data-transport/wiki/etl)

The read/write functions make data-transport a great candidate for **data-science**; **data-engineering** or all things pertaining to data. It enables operations across multiple data-stores(relational or not)

## ETL

**Embedded in Code**

It is possible to perform ETL within custom code as follows :

```
    import transport
    import time
    
    _info = [{source:{'provider':'sqlite','path':'/home/me/foo.csv','table':'me'},target:{provider:'bigquery',private_key='/home/me/key.json','table':'me','dataset':'mydataset'}}, ...]    
    procs = transport.factory.instance(provider='etl',info=_info)
    #
    #
    while procs:
        procs = [pthread for pthread in procs if pthread.is_alive()]
        time.sleep(1)
```

**Command Line Interface (CLI):**
---
The CLI program is called **transport** and it requires a configuration file. The program is intended to move data from one location to another. Supported data stores are in the above paragraphs.

```
[
    {
    "id":"logs",
    "source":{
        "provider":"postgresql","context":"read","database":"mydb",
        "cmd":{"sql":"SELECT * FROM logs limit 10"}
        },
    "target":{
        "provider":"bigquery","private_key":"/bgqdrive/account/bq-service-account-key.json",
        "dataset":"mydataset"
        }    
    },
    
]
```

Assuming the above content is stored in a file called **etl-config.json**, we would perform the following in a terminal window:

```
[steve@data-transport]$ transport --config ./etl-config.json [--index <value>]
```

**Reading/Writing Mongodb**

For this example we assume here we are tunneling through port 27018 and there is not access control:

```
import transport
reader = factory.instance(provider='mongodb',context='read',host='localhost',port='27018',db='example',doc='logs')

df = reader.read() #-- reads the entire collection
print (df.head())
#
#-- Applying mongodb command
PIPELINE = [{"$group":{"_id":None,"count":{"$sum":1}}}]
_command_={"cursor":{},"allowDiskUse":True,"aggregate":"logs","pipeline":PIPLINE}
df = reader.read(mongo=_command)
print (df.head())
reader.close()
```
**Writing to Mongodb**
---
```
import transport
improt pandas as pd
writer = factory.instance(provider='mongodb',context='write',host='localhost',port='27018',db='example',doc='logs')

df = pd.DataFrame({"names":["steve","nico"],"age":[40,30]})
writer.write(df)
writer.close()
```



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
    
