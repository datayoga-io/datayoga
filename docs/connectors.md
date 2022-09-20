---
title: Quickstart
layout: page
nav_order: 3
---

# Connections

## Introduction

`datayoga` supports a wide variety of `connectors` to support external sources including stream providers, relational databases, non-relational databases, blob storage, and external APIs.

the connections are defined in the `connections.yaml`. This file includes a reference to a logical name for each declared connection along with its extra configuration properties and credentials.

Some connectors require installation of optional drivers.

## Supported databases

| Connector | PyPi driver | Used by | Connector URL format |
| [Amazon Athena](/docs/databases/athena) | `store_sql` `extract_sql` | `pip install "PyAthenaJDBC>1.0.9` , `pip install "PyAthena>1.2.0` | `awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com/{ ` |
| [Amazon Redshift](/docs/databases/redshift) | `store_sql` `extract_sql` | `pip install sqlalchemy-redshift` | ` redshift+psycopg2://<userName>:<DBPassword>@<AWS End Point>:5439/<Database Name>` |
| [Apache Drill](/docs/databases/drill) | `store_sql` `extract_sql` | `pip install sqlalchemy-drill` | `drill+sadrill:// For JDBC drill+jdbc://` |
| [Apache Druid](/docs/databases/druid) | `store_sql` `extract_sql` | `pip install pydruid` | `druid://<User>:<password>@<Host>:<Port-default-9088>/druid/v2/sql` |
| [Apache Hive](/docs/databases/hive) | `store_sql` `extract_sql` | `pip install pyhive` | `hive://hive@{hostname}:{port}/{database}` |
| [Apache Impala](/docs/databases/impala) | `store_sql` `extract_sql` | `pip install impyla` | `impala://{hostname}:{port}/{database}` |
| [Apache Kylin](/docs/databases/kylin) | `store_sql` `extract_sql` | `pip install kylinpy` | `kylin://<username>:<password>@<hostname>:<port>/<project>?<param1>=<value1>&<param2>=<value2>` |
| [Apache Pinot](/docs/databases/pinot) | `store_sql` `extract_sql` | `pip install pinotdb` | `pinot://BROKER:5436/query?server=http://CONTROLLER:5983/` |
| [Apache Solr](/docs/databases/solr) | `store_sql` `extract_sql` | `pip install sqlalchemy-solr` | `solr://{username}:{password}@{hostname}:{port}/{server_path}/{collection}` |
| [Apache Spark SQL](/docs/databases/spark-sql) | `store_sql` `extract_sql` | `pip install pyhive` | `hive://hive@{hostname}:{port}/{database}` |
| [Ascend.io](/docs/databases/ascend) | `store_sql` `extract_sql` | `pip install impyla` | `ascend://{username}:{password}@{hostname}:{port}/{database}?auth_mechanism=PLAIN;use_ssl=true` |
| [Azure MS SQL](/docs/databases/sql-server) | `store_sql` `extract_sql` | `pip install pymssql` | `mssql+pymssql://UserName@presetSQL:TestPassword@presetSQL.database.windows.net:1433/TestSchema` |
| [Big Query](/docs/databases/bigquery) | `store_sql` `extract_sql` | `pip install pybigquery` | `bigquery://{project_id}` |
| [ClickHouse](/docs/databases/clickhouse) | `store_sql` `extract_sql` | `pip install clickhouse-sqlalchemy` | `clickhouse+native://{username}:{password}@{hostname}:{port}/{database}` |
| [CockroachDB](/docs/databases/cockroachdb) | `store_sql` `extract_sql` | `pip install cockroachdb` | `cockroachdb://root@{hostname}:{port}/{database}?sslmode=disable` |
| [Dremio](/docs/databases/dremio) | `store_sql` `extract_sql` | `pip install sqlalchemy_dremio` | `dremio://user:pwd@host:31010/` |
| [Elasticsearch](/docs/databases/elasticsearch) | `store_sql` `extract_sql` | `pip install elasticsearch-dbapi` | `elasticsearch+http://{user}:{password}@{host}:9200/` |
| [Exasol](/docs/databases/exasol) | `store_sql` `extract_sql` | `pip install sqlalchemy-exasol` | `exa+pyodbc://{username}:{password}@{hostname}:{port}/my_schema?CONNECTIONLCALL=en_US.UTF-8&driver=EXAODBC` |
| [Google Sheets](/docs/databases/google-sheets) | `store_sql` `extract_sql` | `pip install shillelagh[gsheetsapi]` | `gsheets://` |
| [Firebolt](/docs/databases/firebolt) | `store_sql` `extract_sql` | `pip install firebolt-sqlalchemy` | `firebolt://{username}:{password}@{database} or firebolt://{username}:{password}@{database}/{engine_name}` |
| [Hologres](/docs/databases/hologres) | `store_sql` `extract_sql` | `pip install psycopg2` | `postgresql+psycopg2://<UserName>:<DBPassword>@<Database Host>/<Database Name>` |
| [IBM Db2](/docs/databases/ibm-db2) | `store_sql` `extract_sql` | `pip install ibm_db_sa` | `db2+ibm_db://` |
| [IBM Netezza Performance Server](/docs/databases/netezza) | `store_sql` `extract_sql` | `pip install nzalchemy` | `netezza+nzpy://<UserName>:<DBPassword>@<Database Host>/<Database Name>` |
| [MySQL](/docs/databases/mysql) | `store_sql` `extract_sql` | `pip install mysqlclient` | `mysql://<UserName>:<DBPassword>@<Database Host>/<Database Name>` |
| [Oracle](/docs/databases/oracle) | `store_sql` `extract_sql` | `pip install cx_Oracle` | `oracle://` |
| [PostgreSQL](/docs/databases/postgres) | `store_sql` `extract_sql` | `pip install psycopg2` | `postgresql://<UserName>:<DBPassword>@<Database Host>/<Database Name>` |
| [Trino](/docs/databases/trino) | `store_sql` `extract_sql` | `pip install sqlalchemy-trino` | `trino://{username}:{password}@{hostname}:{port}/{catalog}` |
| [Presto](/docs/databases/presto) | `store_sql` `extract_sql` | `pip install pyhive` | `presto://` |
| [SAP Hana](/docs/databases/hana) | `store_sql` `extract_sql` | `pip install hdbcli sqlalchemy-hana or pip install apache-superset[hana]` | `hana://{username}:{password}@{host}:{port}` |
| [Snowflake](/docs/databases/snowflake) | `store_sql` `extract_sql` | `pip install snowflake-sqlalchemy` | `snowflake://{user}:{password}@{account}.{region}/{database}?role={role}&warehouse={warehouse}` |
| SQLite | `store_sql` `extract_sql` | No additional library needed | `sqlite://` |
| [SQL Server](/docs/databases/sql-server) | `store_sql` `extract_sql` | `pip install pymssql` | `mssql://` |
| [Teradata](/docs/databases/teradata) | `store_sql` `extract_sql` | `pip install teradatasqlalchemy ` | `teradata://{user}:{password}@{host}` |
| [TimescaleDB](/docs/databases/timescaledb) | `store_sql` `extract_sql` | `pip install psycopg2` | `postgresql://<UserName>:<DBPassword>@<Database Host>:<Port>/<Database Name>` |
| [Vertica](/docs/databases/vertica) | `store_sql` `extract_sql` | `pip install sqlalchemy-vertica-python` | `vertica+vertica_python://<UserName>:<DBPassword>@<Database Host>/<Database Name>` |
| [YugabyteDB](/docs/databases/yugabytedb) | `store_sql` `extract_sql` | `pip install psycopg2` | `postgresql://<UserName>:<DBPassword>@<Database Host>/<Database Name>` |

| Amazon S3 | `store_cloud_storage` `extract_cloud_storage` | `pip install boto3` | |
| GCP GS | `store_cloud_storage` `extract_cloud_storage` | `pip install google-cloud-storage` | |
| Azure | `store_cloud_storage` `extract_cloud_storage` | `pip install azure-storage-blob` | |
