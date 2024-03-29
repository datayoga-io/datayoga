---
parent: Connection Types
grand_parent: Reference
---

# DB2

Schema for configuring DB2 database connection parameters


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**type**|`string`|Connection type<br/>Constant Value: `"db2"`<br/>|yes|
|**host**|`string`|DB host<br/>|yes|
|**port**|`integer`|DB port<br/>Minimum: `1`<br/>Maximum: `65535`<br/>|yes|
|**database**|`string`|DB name<br/>|yes|
|**user**|`string`|DB user<br/>|yes|
|**password**|`string`|DB password<br/>|no|
|[**connect\_args**](#connect_args)|`object`|Additional arguments to use when connecting to the DB<br/>|no|
|[**query\_args**](#query_args)|`object`|Additional query string arguments to use when connecting to the DB<br/>|no|

**Example**

```yaml
db2:
  type: db2
  host: localhost
  port: 50000
  database: sample
  user: myuser
  password: mypass
  connect_args:
    ssl_ca: /opt/ssl/ca.crt
    ssl_cert: /opt/ssl/client.crt
    ssl_key: /opt/ssl/client.key

```

<a name="connect_args"></a>
## connect\_args: object

Additional arguments to use when connecting to the DB


**Additional Properties:** allowed  
<a name="query_args"></a>
## query\_args: object

Additional query string arguments to use when connecting to the DB


**Additional Properties:** allowed  

