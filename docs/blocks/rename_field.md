---
parent: Blocks Reference
layout: page
---

# Rename fields

Renames fields. All other fields remain unchanged


   
**Option 1 (alternative):** 
Rename multiple fields


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|[**fields**](#option1fields)|`object[]`|Fields<br/>|yes|

**Additional Properties:** not allowed  
**Example**

```yaml
fields:
  - {}

```


   
**Option 2 (alternative):** 
Rename one field


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**from\_field**|`string`|From field<br/>|yes|
|**to\_field**|`string`|To field<br/>|yes|

**Additional Properties:** not allowed  

<a name="option1fields"></a>
## Option 1: fields\[\]: array

Fields


**Items**

**Item Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**from\_field**|`string`|From field<br/>|yes|
|**to\_field**|`string`|To field<br/>|yes|

**Item Additional Properties:** not allowed  
**Example**

```yaml
- {}

```


