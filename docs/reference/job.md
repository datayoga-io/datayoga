---
parent: Reference
nav_order: 2
---

# Job

Job descriptor


**Items**

**Item Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**uses**|`string`|Block type<br/>|yes|
|[**with**](#itemwith)|`array`|Properties<br/>|yes|

**Item Additional Properties:** not allowed  
**Example**

```yaml
- uses: read_csv
  with:
    file: employees.csv
    batch_size: 2500

```

<a name="itemwith"></a>
### item\.with\[\]: object,array

Properties


**No properties.**


