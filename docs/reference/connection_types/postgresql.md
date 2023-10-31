---
parent: Connection Types
grand_parent: Reference
---

# any

**Example**

```yaml
hr:
  type: postgresql
  host: localhost
  port: 5432
  database: postgres
  user: postgres
  password: postgres
  connect_args:
    connect_timeout: 10
  query_args:
    sslmode: verify-ca
    sslrootcert: /opt/ssl/ca.crt
    sslcert: /opt/ssl/client.crt
    sslkey: /opt/ssl/client.key

```


