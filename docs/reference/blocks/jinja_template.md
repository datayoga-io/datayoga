---
parent: Blocks
grand_parent: Reference
---

# jinja\_template

Apply Jinja template to a field


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**field**|`string`|Field<br/>|yes|
|**template**|`string`|Jinja Template<br/>|yes|

**Additional Properties:** not allowed  
**Example**

```yaml
field: name.full_name
template: '{{ name.fname }} {{ name.lname }}'

```

**Example**

```yaml
field: name.fname_upper
template: '{{ name.fname | upper }}'

```


