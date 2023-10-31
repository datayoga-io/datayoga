---
parent: Connection Types
grand_parent: Reference
---

# Oracle Database Configuration

Schema for configuring Oracle database connection parameters


**Example**

```yaml
hr:
  type: oracle
  host: localhost
  port: 5432
  database: orcl
  user: scott
  password: tiger
  oracle_thick_mode: true
  oracle_thick_mode_lib_dir: /opt/oracle/instantclient_21_8/

```


