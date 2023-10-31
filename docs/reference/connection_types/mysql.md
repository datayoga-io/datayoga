---
parent: Connection Types
grand_parent: Reference
---

# MySQL Database Configuration

Schema for configuring MySQL database connection parameters


**Example**

```yaml
hr:
  type: mysql
  host: localhost
  port: 3306
  database: hr
  user: myuser
  password: mypass
  connect_args:
    ssl_ca: /opt/ssl/ca.crt
    ssl_cert: /opt/ssl/client.crt
    ssl_key: /opt/ssl/client.key

```


