---
parent: Connection Types
grand_parent: Reference
---

# Redis

Schema for configuring Redis database connection parameters


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**type**|`string`|Connection type<br/>Constant Value: `"redis"`<br/>|yes|
|**host**|`string`|Redis DB host<br/>|yes|
|**port**|`integer`|Redis DB port<br/>Minimum: `1`<br/>Maximum: `65535`<br/>|yes|
|**user**|`string`|Redis DB user<br/>|no|
|**password**|`string`|Redis DB password<br/>|no|
|**key**|`string`|Private key file to authenticate with<br/>|no|
|**key\_password**|`string`|Password for unlocking an encrypted private key<br/>|no|
|**cert**|`string`|Client certificate file to authenticate with<br/>|no|
|**cacert**|`string`|CA certificate file to verify with<br/>|no|
|**socket\_timeout**|`number`|Socket timeout in seconds<br/>Default: `10`<br/>Minimum: `0`<br/>|no|
|**socket\_connect\_timeout**|`number`|Socket connection timeout in seconds<br/>Default: `2`<br/>Minimum: `0`<br/>|no|
|**socket\_keepalive**|`boolean`|Enable/disable TCP keepalive<br/>Default: `true`<br/>|no|
|**health\_check\_interval**|`number`|Interval for health checks in seconds<br/>Default: `60`<br/>Minimum: `0`<br/>|no|

**Additional Properties:** not allowed  
**If property *key* is defined**, property/ies *cert* is/are required.  
**If property *cert* is defined**, property/ies *key* is/are required.  
**If property *key_password* is defined**, property/ies *key* is/are required.  
**Example**

```yaml
cache:
  type: redis
  host: localhost
  port: 6379
  socket_timeout: 10
  socket_connect_timeout: 2
  socket_keepalive: true
  health_check_interval: 60

```


