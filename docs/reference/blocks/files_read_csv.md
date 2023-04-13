---
parent: Blocks
grand_parent: Reference
---

# files\.read\_csv

Read data from CSV


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**file**|`string`|Filename. Can contain a regexp or glob expression<br/>|yes|
|**encoding**|`string`|Encoding to use for reading the file<br/>Default: `"utf-8"`<br/>|no|
|[**fields**](#fields)<br/>(List of columns to use)|`string[]`|List of columns to use for extract<br/>Minimal Length: `1`<br/>|no|
|**skip**|`number`|Number of lines to skip<br/>Default: `0`<br/>Minimum: `0`<br/>|no|
|**delimiter**|`string`|Delimiter to use for splitting the csv records<br/>Default: `","`<br/>Minimal Length: `1`<br/>Maximal Length: `1`<br/>|no|
|**batch\_size**|`number`|Number of records to read per batch<br/>Default: `1000`<br/>Minimum: `1`<br/>|no|
|**quotechar**|`string`|A one-character string used to quote fields containing special characters, such as the delimiter or quotechar, or which contain new-line characters. It defaults to '<br/>Default: `"\""`<br/>Minimal Length: `1`<br/>Maximal Length: `1`<br/>|no|

**Additional Properties:** not allowed  
**Example**

```yaml
file: archive.csv
delimiter: ;

```

<a name="fields"></a>
## fields\[\]: List of columns to use

List of columns to use for extract


**Items**


field name

**Item Type:** `string`  
**Example**

```yaml
- fname
- lname

```


