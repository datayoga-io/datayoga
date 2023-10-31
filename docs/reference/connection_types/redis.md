---
parent: Connection Types
grand_parent: Reference
---

# Redis

Schema for configuring Redis database connection parameters


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**type**|`string`|DB type<br/>Enum: `"redis"`<br/>|yes|
|**host**|`string`|Redis DB host<br/>|yes|
|**port**|`integer`|Redis DB port<br/>Minimum: `1`<br/>Maximum: `65535`<br/>|yes|
|**user**|`string`|Redis DB user<br/>|no|
|**password**|`string`|Redis DB password<br/>|no|
|**key**<br/>(Private key file to authenticate with)|`string`||no|
|**key\_password**<br/>(Password for unlocking an encrypted private key)|`string`||no|
|**cert**<br/>(Client certificate file to authenticate with)|`string`||no|
|**cacert**<br/>(CA certificate file to verify with)|`string`||no|

**Additional Properties:** not allowed  
**If property *key* is defined**, property/ies *cert* is/are required.  
**If property *cert* is defined**, property/ies *key* is/are required.  
**If property *key_password* is defined**, property/ies *key* is/are required.  
**If property *user* is defined**, property/ies *password* is/are required.  
**Example**

```yaml
cache:
  type: redis
  host: localhost
  port: 6379

```


