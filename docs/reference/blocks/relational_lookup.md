---
parent: Blocks
grand_parent: Reference
---

# relational\.write

Lookup in a relational table


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**condition**<br/>(The lookup condition)|`string`|Use any valid SQL syntax. Use the alias `lookup` for the lookup table and `incoming` for the base table<br/>|no|
|**query**<br/>(Query string to use as an override to the built in query)|`string`|Use any valid SQL syntax. Use the alias `lookup` for the lookup table and `incoming` for the base table<br/>|no|
|**schema**<br/>(The table schema of the lookup table)|`string`|If not specified, no specific schema will be used when connecting to the database.<br/>|no|
|**table**<br/>(The lookup table name)|`string`|Lookup table name<br/>|no|
|[**order\_by**](#order_by)<br/>(List of keys to use for ordering\. Applicable for multiple matches)|`array`||no|
|[**fields**](#fields)<br/>(Columns to add to the output from the lookup table)|`array`||no|
|**multiple\_match\_policy**|`string`|How to handle multiple matches in the lookup table<br/>Default: `""`<br/>Enum: `"first"`, `"last"`, `"all"`<br/>|no|

**Additional Properties:** not allowed  
**Example**

```yaml
id: load_snowflake
type: relational.write
properties:
  connection: eu_datalake
  table: employees
  schema: dbo
  load_strategy: APPEND

```

<a name="order_by"></a>
## order\_by\[\]: List of keys to use for ordering\. Applicable for multiple matches

**Items: name of column**

**Item Type:** `string`  
**Example**

```yaml
- country_name

```

<a name="fields"></a>
## fields\[\]: Columns to add to the output from the lookup table

**Items: name of column**

**No properties.**

**Example**

```yaml
- fname
- lname: last_name
- address
- gender

```


