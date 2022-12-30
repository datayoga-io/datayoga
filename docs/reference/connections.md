---
parent: Reference
nav_order: 1
---

# Connections

Connection catalog


**Properties (Pattern)**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|[**\.**](#)|`object`|||

**Additional Properties:** not allowed  
<a name=""></a>
### \.: object

   
**Option 1 (optional):** 
SQL database


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**type**|`string`|DB type<br/>Enum: `"mssql"`, `"mysql"`, `"postgresql"`<br/>|yes|
|**host**|`string`|DB host<br/>|yes|
|**port**|`integer`|DB port<br/>Minimum: `1`<br/>Maximum: `65535`<br/>|no|
|**database**|`string`|DB name<br/>|yes|
|**user**|`string`|DB user<br/>|yes|
|**password**|`string`|DB password<br/>|no|
|[**connect\_args**](#option1connect_args)|`object`|Additional arguments to use when connecting to the DB<br/>|no|

**Additional Properties:** not allowed  
**Example**

```yaml
hr:
  type: postgresql
  host: localhost
  port: 5432
  database: postgres
  user: postgres
  password: postgres
  connect_args:
    connect_timeout: 10

```


   
**Option 2 (optional):** 
Redis


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**type**|`string`|DB type<br/>Enum: `"redis"`<br/>|yes|
|**host**|`string`|Redis DB host<br/>|yes|
|**port**|`integer`|Redis DB port<br/>Minimum: `1`<br/>Maximum: `65535`<br/>|yes|
|**password**|`string`|Redis DB password<br/>|no|

**Additional Properties:** not allowed  
**Example**

```yaml
cache:
  type: redis
  host: localhost
  port: 6379

```


   
**Option 3 (optional):** 
Cassandra


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**type**|`string`|DB type<br/>Enum: `"cassandra"`<br/>|yes|
|[**hosts**](#option3hosts)|`string[]`|Cassandra hosts<br/>|yes|
|**port**|`integer`|Cassandra DB port<br/>Default: `9042`<br/>Minimum: `1`<br/>Maximum: `65535`<br/>|no|
|**database**|`string`|DB name<br/>|no|
|**user**|`string`|DB user<br/>|no|
|**password**|`string`|DB password<br/>|no|

**Additional Properties:** not allowed  
**Example**

```yaml
cache:
  type: cassandra
  hosts:
    - localhost
  port: 9042
  database: myDB
  user: myUser
  password: myPassword

```


<a name="option1connect_args"></a>
## Option 1\]: connect\_args: object

Additional arguments to use when connecting to the DB


**No properties.**

<a name="option3hosts"></a>
## Option 3\]: hosts\[\]: array

Cassandra hosts


**Items: Address of Cassandra node**

**Item Type:** `string`  

