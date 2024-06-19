# Introduction

This project implements an abstraction of objects that can have access to a variety of data stores, implementing read/write with a simple and expressive interface. This abstraction works with **NoSQL**, **SQL** and **Cloud** data stores and leverages **pandas**.

# Why Use Data-Transport ?

Mostly data scientists that don't really care about the underlying database and would like a simple and consistent way to read/write and move data are well served. Additionally we implemented lightweight Extract Transform Loading API and command line (CLI) tool. Finally it is possible to add pre/post processing pipeline functions to read/write

1. Familiarity with **pandas data-frames**
2. Connectivity **drivers** are included
3. Reading/Writing data from various sources
4. Useful for data migrations or **ETL**


## Installation

Within the virtual environment perform the following :

    pip install git+https://github.com/lnyemba/data-transport.git


## What's new

Unlike older versions 2.0 and under, we focus on collaborative environments like jupyter-x servers; apache zeppelin:

    1. Simpler syntax to create reader or writer
    2. auth-file registry that can be referenced using a label


## Learn More

We have available notebooks with sample code to read/write against mongodb, couchdb, Netezza, PostgreSQL, Google Bigquery, Databricks, Microsoft SQL Server, MySQL ... Visit [data-transport homepage](https://healthcareio.the-phi.com/data-transport)
