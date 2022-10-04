---
parent: Blocks
grand_parent: Reference
---

# Map Record

Maps a record into a new output based on expressions

**Properties**

| Name                          | Type     | Description                                   | Required |
| ----------------------------- | -------- | --------------------------------------------- | -------- |
| [**expression**](#expression) | `object` | Expression<br/>                               | yes      |
| **language**                  | `string` | Language<br/>Enum: `"jmespath"`, `"sql"`<br/> | yes      |

**Additional Properties:** not allowed  
**Example**

```yaml
expression:
  first_name: first_name
  last_name: last_name
  greeting: >-
    'Hello ' || CASE WHEN gender = 'F' THEN 'Ms.' WHEN gender = 'M' THEN 'Mr.'
    ELSE 'N/A' END || ' ' || full_name
  country: country
  full_name: full_name
language: sql
```

<a name="expression"></a>

## expression: object

Expression

**No properties.**
