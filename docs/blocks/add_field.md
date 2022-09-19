---
parent: Blocks Reference
layout: page
---

# Add fields

Add fields to a record


   
**Option 1 (alternative):** 
Add multiple fields


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|[**fields**](#option1fields)|`object[]`|Fields<br/>|yes|

**Additional Properties:** not allowed  
**Example**

```yaml
fields:
  - field: name.full_name
    language: jmespath
    expression: concat([name.fname, ' ', name.lname])
  - field: name.fname_upper
    language: jmespath
    expression: upper(name.fname)

```


   
**Option 2 (alternative):** 
Add one field


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**field**|`string`|Field<br/>|yes|
|**expression**|`string`|Expression<br/>|yes|
|**language**|`string`|Language<br/>Enum: `"jmespath"`, `"sql"`<br/>|yes|

**Additional Properties:** not allowed  
**Example**

```yaml
field: country
language: sql
expression: country_code || ' - ' || UPPER(country_name)

```


<a name="option1fields"></a>
## Option 1: fields\[\]: array

Fields


**Items**

**Item Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**field**|`string`|Field<br/>|yes|
|**expression**|`string`|Expression<br/>|yes|
|**language**|`string`|Language<br/>Enum: `"jmespath"`, `"sql"`<br/>|yes|

**Item Additional Properties:** not allowed  
**Example**

```yaml
- {}

```


