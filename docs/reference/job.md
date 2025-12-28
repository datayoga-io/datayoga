---
parent: Reference
nav_order: 2
---

# Job

Job descriptor


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|[**input**](#input)|`object`||yes|
|[**steps**](#steps)|`array`|||
|**error\_handling**|`string`|Error handling strategy: abort - terminate job, ignore - skip<br/>Default: `"ignore"`<br/>Enum: `"abort"`, `"ignore"`<br/>||

**Additional Properties:** not allowed  
**Example**

```yaml
input: &ref_0
  uses: files.read_csv
  with:
    file: employees.csv
    batch_size: 2500
steps:
  - *ref_0
error_handling: ignore

```

<a name="input"></a>
## input: object

**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**uses**|`string`|Block type<br/>|yes|
|[**with**](#inputwith)|`array`|Properties<br/>|no|

**Additional Properties:** not allowed  
**Example**

```yaml
uses: files.read_csv
with:
  file: employees.csv
  batch_size: 2500

```

**Example**

```yaml
uses: add_field
with:
  field: full_name
  language: jmespath
  expression: concat([capitalize(fname), ' ' , capitalize(lname)])

```

**Example**

```yaml
uses: map
with:
  expression:
    id: id
    full_name: full_name
    country: country_code || ' - ' || UPPER(country_name)
    gender: gender

```

**Example**

```yaml
uses: redis.write
with:
  connection: cache
  command: HSET
  key:
    expression: id
    language: jmespath

```

<a name="inputwith"></a>
### input\.with\[\]: object,array

Properties


**No properties.**

<a name="steps"></a>
## steps\[\]: array

**Items**

**Example**

```yaml
- uses: files.read_csv
  with:
    file: employees.csv
    batch_size: 2500

```


