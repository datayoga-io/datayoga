---
nav_order: 7
---

# Connectors

## Introduction

`datayoga` supports a wide variety of `connectors` to support external sources including stream providers, relational databases, non-relational databases, blob storage, and external APIs.

the connections are defined in the `connections.yaml`. This file includes a reference to a logical name for each declared connection along with its extra configuration properties and credentials.

Some connectors require installation of optional drivers.

## Connections.yaml Example

Example

```yaml
dwh:
  type: postgresql
  username: pg
  password: ${oc.env:PG_PWD}
  host: localhost
  port: 5432
  database: rww
```

## Supported Connectors

| Connector                                                 | Used by                                       | PyPi Driver                                                               | Connector Properties                                                                                        |
| --------------------------------------------------------- | --------------------------------------------- | ------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| [Amazon Athena](/docs/databases/athena)                   | `read_sql` `write_sql`                        | `pip install "PyAthenaJDBC>1.0.9` , `pip install "PyAthena>1.2.0`         | `aws_access_key_id` `aws_secret_access_key` `region_name`                                                   |
| [Amazon Redshift](/docs/databases/redshift)               | `read_sql` `write_sql`                        | `pip install sqlalchemy-redshift`                                         | `username` `password` `aws_end_point` `database`                                                            |
| [Apache Drill](/docs/databases/drill)                     | `read_sql` `write_sql`                        | `pip install sqlalchemy-drill`                                            |                                                                                                             |
| [Apache Druid](/docs/databases/druid)                     | `read_sql` `write_sql`                        | `pip install pydruid`                                                     | `username` `password` `host` `port`                                                                         |
| [Apache Hive](/docs/databases/hive)                       | `read_sql` `write_sql`                        | `pip install pyhive`                                                      | `host` `port` `database`                                                                                    |
| [Apache Impala](/docs/databases/impala)                   | `read_sql` `write_sql`                        | `pip install impyla`                                                      | `host` `port` `database`                                                                                    |
| [Apache Kylin](/docs/databases/kylin)                     | `read_sql` `write_sql`                        | `pip install kylinpy`                                                     | `host` `port` `database` `password` `project`                                                               |
| [Apache Pinot](/docs/databases/pinot)                     | `read_sql` `write_sql`                        | `pip install pinotdb`                                                     | `broker` `server`                                                                                           |
| [Apache Solr](/docs/databases/solr)                       | `read_sql` `write_sql`                        | `pip install sqlalchemy-solr`                                             | `username` `password` `host` `port` `server_path` `collection`                                              |
| [Apache Spark SQL](/docs/databases/spark-sql)             | `read_sql` `write_sql`                        | `pip install pyhive`                                                      | `host` `port` `database`                                                                                    |
| [Ascend.io](/docs/databases/ascend)                       | `read_sql` `write_sql`                        | `pip install impyla`                                                      | `host` `port` `database`                                                                                    |
| [Azure MS SQL](/docs/databases/sql-server)                | `read_sql` `write_sql`                        | `pip install pymssql`                                                     | `mssql+pymssql://UserName@presetSQL:TestPassword@presetSQL.database.windows.net:1433/TestSchema`            |
| [Big Query](/docs/databases/bigquery)                     | `read_sql` `write_sql`                        | `pip install pybigquery`                                                  | `bigquery://{project_id}`                                                                                   |
| [ClickHouse](/docs/databases/clickhouse)                  | `read_sql` `write_sql`                        | `pip install clickhouse-sqlalchemy`                                       | `clickhouse+native://{username}:{password}@{hostname}:{port}/{database}`                                    |
| [CockroachDB](/docs/databases/cockroachdb)                | `read_sql` `write_sql`                        | `pip install cockroachdb`                                                 | `cockroachdb://root@{hostname}:{port}/{database}?sslmode=disable`                                           |
| [Dremio](/docs/databases/dremio)                          | `read_sql` `write_sql`                        | `pip install sqlalchemy_dremio`                                           | `dremio://user:pwd@host:31010/`                                                                             |
| [Elasticsearch](/docs/databases/elasticsearch)            | `read_sql` `write_sql`                        | `pip install elasticsearch-dbapi`                                         | `elasticsearch+http://{user}:{password}@{host}:9200/`                                                       |
| [Exasol](/docs/databases/exasol)                          | `read_sql` `write_sql`                        | `pip install sqlalchemy-exasol`                                           | `exa+pyodbc://{username}:{password}@{hostname}:{port}/my_schema?CONNECTIONLCALL=en_US.UTF-8&driver=EXAODBC` |
| [Google Sheets](/docs/databases/google-sheets)            | `read_sql` `write_sql`                        | `pip install shillelagh[gsheetsapi]`                                      | `gsheets://`                                                                                                |
| [Firebolt](/docs/databases/firebolt)                      | `read_sql` `write_sql`                        | `pip install firebolt-sqlalchemy`                                         | `firebolt://{username}:{password}@{database} or firebolt://{username}:{password}@{database}/{engine_name}`  |
| [Hologres](/docs/databases/hologres)                      | `read_sql` `write_sql`                        | `pip install psycopg2`                                                    | `postgresql+psycopg2://<UserName>:<DBPassword>@<Database Host>/<Database Name>`                             |
| [IBM Db2](/docs/databases/ibm-db2)                        | `read_sql` `write_sql`                        | `pip install ibm_db_sa`                                                   | `db2+ibm_db://`                                                                                             |
| [IBM Netezza Performance Server](/docs/databases/netezza) | `read_sql` `write_sql`                        | `pip install nzalchemy`                                                   | `netezza+nzpy://<UserName>:<DBPassword>@<Database Host>/<Database Name>`                                    |
| [MySQL](/docs/databases/mysql)                            | `read_sql` `write_sql`                        | `pip install mysqlclient`                                                 | `mysql://<UserName>:<DBPassword>@<Database Host>/<Database Name>`                                           |
| [Oracle](/docs/databases/oracle)                          | `read_sql` `write_sql`                        | `pip install cx_Oracle`                                                   | `oracle://`                                                                                                 |
| [PostgreSQL](/docs/databases/postgres)                    | `read_sql` `write_sql`                        | `pip install psycopg2`                                                    | `postgresql://<UserName>:<DBPassword>@<Database Host>/<Database Name>`                                      |
| [Trino](/docs/databases/trino)                            | `read_sql` `write_sql`                        | `pip install sqlalchemy-trino`                                            | `trino://{username}:{password}@{hostname}:{port}/{catalog}`                                                 |
| [Presto](/docs/databases/presto)                          | `read_sql` `write_sql`                        | `pip install pyhive`                                                      | `presto://`                                                                                                 |
| [SAP Hana](/docs/databases/hana)                          | `read_sql` `write_sql`                        | `pip install hdbcli sqlalchemy-hana or pip install apache-superset[hana]` | `hana://{username}:{password}@{host}:{port}`                                                                |
| [Snowflake](/docs/databases/snowflake)                    | `read_sql` `write_sql`                        | `pip install snowflake-sqlalchemy`                                        | `snowflake://{user}:{password}@{account}.{region}/{database}?role={role}&warehouse={warehouse}`             |
| SQLite                                                    | `read_sql` `write_sql`                        | No additional library needed                                              | `sqlite://`                                                                                                 |
| [SQL Server](/docs/databases/sql-server)                  | `read_sql` `write_sql`                        | `pip install pymssql`                                                     | `mssql://`                                                                                                  |
| [Teradata](/docs/databases/teradata)                      | `read_sql` `write_sql`                        | `pip install teradatasqlalchemy `                                         | `teradata://{user}:{password}@{host}`                                                                       |
| [TimescaleDB](/docs/databases/timescaledb)                | `read_sql` `write_sql`                        | `pip install psycopg2`                                                    | `username` `password` `host` `port` `database`                                                              |
| [Vertica](/docs/databases/vertica)                        | `read_sql` `write_sql`                        | `pip install sqlalchemy-vertica-python`                                   | `vertica+vertica_python://<UserName>:<DBPassword>@<Database Host>/<Database Name>`                          |
| [YugabyteDB](/docs/databases/yugabytedb)                  | `read_sql` `write_sql`                        | `pip install psycopg2`                                                    | `postgresql://<UserName>:<DBPassword>@<Database Host>/<Database Name>`                                      |
| Amazon S3                                                 | `write_cloud_storage` `extract_cloud_storage` | `pip install boto3`                                                       |                                                                                                             |
| GCP GS                                                    | `write_cloud_storage` `extract_cloud_storage` | `pip install google-cloud-storage`                                        |                                                                                                             |
| Azure                                                     | `write_cloud_storage` `extract_cloud_storage` | `pip install azure-storage-blob`                                          |                                                                                                             |
| Redis                                                     | `read_redis` `write_redis`                    | `pip install redis`                                                       |                                                                                                             |
| MongoDB                                                   | `read_mongodb` `write_mongodb`                | `pip install pymongo`                                                     |                                                                                                             |
| ElasticSearch                                             | `read_elasticsearch` `write_elasticsearch`    | `pip install elasticsearch`                                               | `nodes` `basic_auth` `ca_certs` `api_key` `bearer_auth`                                                     |

## Interpolation

DataYoga supports variable interpolation. The interpolated variable can be the path to another node in the configuration, and in that case the value will be the value of that node. This path may use either dot-notation (foo.1), brackets ([foo][1]) or a mix of both (foo[1], [foo].1).

```yaml
pg1:
  type: postgresql
  username: pg
  password: ${}
  host: localhost
  port: 5432
  database: rww
pg2:
  type: ${pg1.type}
  username: pg
  password: ${}
  port: ${pg1.port}
  host: localhost
```

Interpolations are absolute by default. Relative interpolation are prefixed by one or more dots: The first dot denotes the level of the node itself and additional dots are going up the parent hierarchy. e.g. ${..foo} points to the foo sibling of the parent of the current node.

## Environment Variables

Access to environment variables is supported using `env:`

Example:

```
pg1:
    pwd: ${env:PG}
```

It is possible to provide a default value in case the variable is not set:

```
pg1:
    host: ${env:PG_HOST,localhost}
```

## Secrets

Access to secrets stored in tmpfs is supported using `file:`

The file should contain `KEY=VALUE` lines

Example:

```yaml
pg1:
  pwd: ${file:/tmpfs/credentials:PWD}
```

It is possible to provide a default value in case the value is not set:

```yaml
pg1:
  pwd: ${file:/tmpfs/credentials:PWD,12345}
```
