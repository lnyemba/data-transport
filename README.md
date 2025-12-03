# Introduction

This project implements an abstraction of objects that can have access to a variety of data stores, implementing read/write with a simple and expressive interface. This abstraction works with **NoSQL**, **SQL** and **Cloud** data stores and leverages **pandas**.

# Why Use Data-Transport ?

Data transport is a simple framework that enables read/write to multiple databases or technologies that can hold data. In using **data-transport**, you are able to:

- Enjoy the simplicity of **data-transport** because it leverages SQLAlchemy & Pandas data-frames.
- Share notebooks and code without having to disclosing database credentials. 
- Seamlessly and consistently access to multiple database technologies at no cost
- No need to worry about accidental writes to a database leading to inconsistent data
- Implement consistent pre and post processing as a pipeline i.e aggregation of functions
- **data-transport** is open-source under MIT License https://github.com/lnyemba/data-transport

## Installation

Within the virtual environment perform the following, the options for installation are:

**sql**       - by default postgresql, mysql, sqlserver, sqlite3+, duckdb 

    pip install data-transport[cloud,nosql,other,all]git+https://github.com/lnyemba/data-transport.git

Options to install components in square brackets, these components are 

**warehouse** - Apache Iceberg, Apache Drill

**cloud**  - to support nextcloud, s3

**nosql** - support for mongodb, couchdb

**other**  - support for files, rabbitmq, http

    pip install data-transport[nosql,cloud,warehouse,all]@git+https://github.com/lnyemba/data-transport.git

## Additional features

    - In addition to read/write, there is support for functions for pre/post processing
    - CLI interface to add to registry, run ETL
    - scales and integrates into shared environments like apache zeppelin; jupyterhub; SageMaker; ...

## Learn More

We have available notebooks with sample code to read/write against mongodb, couchdb, Netezza, PostgreSQL, Google Bigquery, Databricks, Microsoft SQL Server, MySQL ... Visit [data-transport homepage](https://healthcareio.the-phi.com/data-transport)
