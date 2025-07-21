# Introduction

This project implements an abstraction of objects that can have access to a variety of data stores, implementing read/write with a simple and expressive interface. This abstraction works with **NoSQL**, **SQL** and **Cloud** data stores and leverages **pandas**.

# Why Use Data-Transport ?

Data transport is a simple framework that:
- easy to install & modify (open-source)
- enables access to multiple database technologies (pandas, SQLAlchemy)
- enables notebook sharing without exposing database credential.
- supports pre/post processing specifications (pipeline)


## Installation

Within the virtual environment perform the following :

    pip install git+https://github.com/lnyemba/data-transport.git

Options to install components in square brackets

    pip install data-transport[nosql,cloud,warehouse,all]@git+https://github.com/lnyemba/data-transport.git


## Additional features

    - In addition to read/write, there is support for functions for pre/post processing
    - CLI interface to add to registry, run ETL
    - scales and integrates into shared environments like apache zeppelin; jupyterhub; SageMaker; ...


## Learn More

We have available notebooks with sample code to read/write against mongodb, couchdb, Netezza, PostgreSQL, Google Bigquery, Databricks, Microsoft SQL Server, MySQL ... Visit [data-transport homepage](https://healthcareio.the-phi.com/data-transport)
