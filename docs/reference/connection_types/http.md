---
parent: Connection Types
grand_parent: Reference
---

# HTTP Connection

Schema for configuring HTTP connection parameters


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**type**|`string`|Connection type<br/>Constant Value: `"http"`<br/>|yes|
|**base\_uri**|`string`|Base URI for the API endpoint<br/>|yes|
|[**headers**](#headers)|`object`|HTTP headers, including authorization token<br/>|no|
|[**query\_parameters**](#query_parameters)|`object`|Default query parameters for all API endpoints<br/>|no|
|**timeout**|`integer`|Timeout for HTTP connection in seconds<br/>Default: `10`<br/>|no|

**Additional Properties:** not allowed  
**Example**

```yaml
base_uri: https://api.example.com
headers:
  Authorization:
    expression: concat([ 'Bearer ', token])
    language: jmespath
  Content-Type: application/json
query_parameters:
  id:
    expression: user_id
    language: jmespath
  timestamp:
    expression: date('now')
    language: sql

```

<a name="headers"></a>
## headers: object

HTTP headers, including authorization token


**Additional Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|

<a name="query_parameters"></a>
## query\_parameters: object

Default query parameters for all API endpoints


**Additional Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|


