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
- - uses: files.read_csv
    with:
      file: employees.csv
      batch_size: 2500
  - uses: add_field
    with:
      field: full_name
      language: jmespath
      expression: concat([capitalize(fname), ' ' , capitalize(lname)])
  - uses: map
    with:
      expression:
        id: id
        full_name: full_name
        country: country_code || ' - ' || UPPER(country_name)
        gender: gender
  - uses: redis.write
    with:
      connection: cache
      command: HSET
      key_field: id

```

<a name="itemwith"></a>
### item\.with\[\]: object,array

Properties


**No properties.**


