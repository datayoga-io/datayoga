---
parent: Reference
nav_order: 1
---

# Connections

Connection catalog


**Properties (Pattern)**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|[**\.**](#)|`object`||yes|

<a name=""></a>
### \.: object

**Required Properties:**

  * type
   
**IF** object

  * has properties, where<br/>property **type** 
    * is `cassandra` or 
    * is `mysql` or 
    * is `postgresql` or 
    * is `sqlserver` 

**THEN**


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**driver**|`string`|Explicit driver to use, if not using default<br/>||
|[**connect\_args**](#thenconnect_args)|`object`|Additional arguments to use when connecting to the DB<br/>||
|[**query\_args**](#thenquery_args)|`object`|Additional query string arguments to use when connecting to the DB<br/>||
|**debug**<br/>(Debug mode)|`boolean`|Echo all SQL commands to stdout<br/>Default: `false`<br/>||

**Example**

```yaml
connect_args: {}
query_args: {}
debug: false

```


<a name="thenconnect_args"></a>
##### \.\.then\.connect\_args: object

Additional arguments to use when connecting to the DB


**No properties.**

<a name="thenquery_args"></a>
##### \.\.then\.query\_args: object

Additional query string arguments to use when connecting to the DB


**No properties.**


