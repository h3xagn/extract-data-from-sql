# Extract data from SQL Server

## Overview

Extracting data every time from a constrained, on-prem source system by several different teams cannot be allowed. The design principle is to build one data pipeline that will extract data from the source system and to never read the same data twice from the source. This data will be moved to a cloud platform where it can be consumed multiple times. This is much more efficient and secure at the end of the day.

To minimise the impact on the source system and bandwidth of the site, we will restore the weekly database backups to another non-critical SQL Server on-prem. From there, we will use the SQL Server Command Line Utility, `sqlcmd`, to extract CSV data from the two tables directly. The CSV files will then be converted into parquet files for uploading to Azure. The blog post with more detail is available here:
- [Extract dark data from SQL Server](https://h3xagn.com/extract-dark-data-from-sql-server/)

## Repo structure and file descriptions

- `main.py`: Simple Python script to launch `sqlcmd`, extract CSV files and convert them to parquet format.

The dependencies and virtual environment details are located in the `Pipfile` which can be used with `pipenv`.

Remember to install [SQL Server Command Line Utilities](https://docs.microsoft.com/en-us/sql/tools/sqlcmd-utility?view=sql-server-ver16).

## License

GNU GPL v3

## Author

[Coenraad Pretorius](https://h3xagn.com/coenraad-pretorius/)