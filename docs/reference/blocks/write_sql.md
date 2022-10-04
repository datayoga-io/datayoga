---
parent: Blocks
grand_parent: Reference
---

# Write SQL

Write into a SQL-compatible data store

**Properties**

| Name                                                                                                                                                      | Type       | Description                                                                                                                                                     | Required |
| --------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| [**business_keys**](#business_keys)<br/>(Business keys to use for upsert in case of an UPSERT)                                                            | `string[]` |                                                                                                                                                                 | no       |
| **connection**<br/>(The connection to use for loading)                                                                                                    | `string`   | Logical connection name as defined in the connections.yaml<br/>                                                                                                 | yes      |
| **table**<br/>(The target table name)                                                                                                                     | `string`   | Target table name<br/>                                                                                                                                          | yes      |
| **schema**<br/>(The table schema of the target table)                                                                                                     | `string`   | If left blank, the default schema of this connection will be used as defined in the connections.yaml<br/>                                                       | yes      |
| **load_strategy**                                                                                                                                         | `string`   | type of target<br/>Default: `"APPEND"`<br/>Enum: `"APPEND"`, `"REPLACE"`, `"UPSERT"`, `"TYPE2"`<br/>                                                            | no       |
| **active_record_indicator**                                                                                                                               | `string`   | Used for `TYPE2` load_strategy. An SQL expression used to identify which rows are active<br/>                                                                   | no       |
| [**inactive_record_mapping**](#inactive_record_mapping)<br/>(Used for \`TYPE2\` load_strategy\. The columns mapping to use to close out an active record) | `array`    | A list of columns to use. Use any valid SQL expression for the source. If 'target' is omitted, will default to the name of the source column<br/>Default: <br/> | no       |

**Additional Properties:** not allowed  
**Example**

```yaml
id: load_snowflake
type: write_sql
properties:
  connection: eu_datalake
  table_name: employees
  table_schema: dbo
  target_type: database
  load_strategy: APPEND
```

<a name="business_keys"></a>

## business_keys\[\]: Business keys to use for upsert in case of an UPSERT

**Items: name of column**

The business key is used for performing an upsert in case the load strategy is UPSERT

**Item Type:** `string`  
<a name="inactive_record_mapping"></a>

## inactive_record_mapping\[\]: Used for \`TYPE2\` load_strategy\. The columns mapping to use to close out an active record

A list of columns to use. Use any valid SQL expression for the source. If 'target' is omitted, will default to the name of the source column

**No properties.**

**Example**

```yaml
- source: CURRENT_DATE
  target: deletedAt
- source: "'Y'"
  target: is_active
```
