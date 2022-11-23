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
|**type**|`string`|DB type<br/>Pattern: ^\(?\!redis$\)<br/>|yes|
|**host**|`string`|DB host<br/>|yes|
|**port**|`integer`|DB port<br/>Minimum: `1`<br/>Maximum: `65535`<br/>|no|
|**database**|`string`|DB name<br/>|yes|
|**user**|`string`|DB user<br/>|yes|
|**password**|`string`|DB password<br/>|no|

**Additional Properties:** not allowed  
**Example**

```yaml
name: hr
type: postgresql
host: localhost
port: 5432
database: postgres
user: postgres
password: postgres

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



