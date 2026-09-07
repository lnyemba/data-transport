# Data-Transport

A powerful abstraction layer for seamless data communication across diverse systems. **data-transport** allows you to interact with NoSQL, SQL, and Cloud storage using a consistent interface powered by **Pandas** and **SQLAlchemy**.

## Why Choose data-transport?

*   **Unified Interface:** Connect to PostgreSQL, MySQL, MongoDB, S3, etc., using the same consistent code.
*   **Security First:** Prevents the dissipation of database connectivity information to protect against security breaches.
*   **Simplicity & Power:** Leverages Pandas DataFrames and SQLAlchemy for intuitive data manipulation.
*   **Robust Pipelines:** Easily integrate pre-processing and post-processing as unified pipelines.
*   **CLI Integration:** Includes a dedicated CLI for registry management and ETL task execution.

## Supported Features

| Component | Technologies Covered |
| :--- | :--- |
| **SQL** | PostgreSQL, MySQL, SQL Server, SQLite3+, DuckDB |
| **NoSQL** | MongoDB, CouchDB |
| **Warehouse** | Apache Iceberg, Apache Drill |
| **Cloud** | Nextcloud, S3 |
| **Other** | Files, RabbitMQ, HTTP |

## Installation

Install the core package and your desired components:

```bash
# Basic installation with default SQL support
pip install data-transport@git+https://github.com/lnyemba/data-transport

# Full suite (SQL, NoSQL, Cloud, Warehouse)
pip install "data-transport[nosql,cloud,warehouse,all]"@git+https://github.com/lnyemba/data-transport.git
```

## Advanced Capabilities
*   **Automated Pipelines:** Seamlessly aggregate functions for data cleaning and transformation.
*   **Portability:** Share notebooks and scripts without exposing raw credentials.
*   **Scalable Integration:** Compatible with environments like Apache Zeppelin, JupyterHub, and SageMaker.

---
[Learn More at the Project Website](https://healthcareio.the-phi.com/data-transport)
License: [MIT](https://github.com/lnyemba/data-transport)
