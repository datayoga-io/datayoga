---
parent: Blocks
grand_parent: Reference
---

# http\.write

Write data using an HTTP request


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**connection**<br/>(The connection to use for the HTTP request)|`string`|Logical connection name as defined in the connections.dy.yaml<br/>|yes|
|**endpoint**|||yes|
|**method**<br/>(HTTP Method)|`string`|HTTP method to be used for the request<br/>Enum: `"GET"`, `"PUT"`, `"POST"`, `"DELETE"`<br/>|yes|
|[**payload**](#payload)<br/>(Request Payload)|`object`|Data to be sent in the request body<br/>|no|
|[**extra\_headers**](#extra_headers)<br/>(Additional HTTP Headers)|`object`|Extra headers to be included in the HTTP request<br/>|no|
|[**extra\_query\_parameters**](#extra_query_parameters)|`object`|Extra query parameters to be included in the HTTP request<br/>|no|
|**timeout**<br/>(Timeout in Seconds)|`integer`|Timeout duration for this specific HTTP request in seconds<br/>|no|
|[**output**](#output)|`object`||no|

**Example**

```yaml
connection: http_example
endpoint:
  expression: concat(['users/', id])
  language: jmespath
method: PUT
payload:
  full_name:
    expression: full_name
    language: jmespath
  greeting:
    expression: greeting
    language: jmespath
extra_headers:
  my_header:
    expression: lname || '-' || fname
    language: sql
extra_query_parameters:
  fname:
    expression: UPPER(fname)
    language: sql
output:
  status_code: response.status_code
  headers: response.headers
  body: response.content
timeout: 3

```

<a name="payload"></a>
## payload: Request Payload

Data to be sent in the request body


**Additional Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**Additional Properties**||||

<a name="extra_headers"></a>
## extra\_headers: Additional HTTP Headers

Extra headers to be included in the HTTP request


**Additional Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**Additional Properties**||||

<a name="extra_query_parameters"></a>
## extra\_query\_parameters: object

Extra query parameters to be included in the HTTP request


**Additional Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**Additional Properties**||||

<a name="output"></a>
## output: object

**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**status\_code**<br/>(Status Code Field Name)|`string`|Name of the field where the HTTP response status code will be stored after the request<br/>||
|**headers**<br/>(Headers Field Name)|`string`|Name of the field where the HTTP response headers will be stored after the request<br/>||
|**body**<br/>(Body Field Name)|`string`|Name of the field where the HTTP response content will be stored after the request<br/>||


