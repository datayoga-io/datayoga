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

<a name=""></a>
### \.: object

**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**type**|`string`|DB type<br/>Enum: `"cassandra"`, `"mysql"`, `"postgresql"`, `"redis"`, `"sqlserver"`<br/>||
|**driver**|`string`|Explicit driver to use, if not using default<br/>||
|[**connect\_args**](#connect_args)|`object`|Additional arguments to use when connecting to the DB<br/>||
|[**query\_args**](#query_args)|`object`|Additional query string arguments to use when connecting to the DB<br/>||
|**debug**<br/>(Debug mode)|`boolean`|Echo all SQL commands to stdout<br/>Default: `false`<br/>||

**Example**

```yaml
connect_args: {}
query_args: {}
debug: false

```

<a name="connect_args"></a>
#### \.\.connect\_args: object

Additional arguments to use when connecting to the DB


**No properties.**

<a name="query_args"></a>
#### \.\.query\_args: object

Additional query string arguments to use when connecting to the DB


**No properties.**


