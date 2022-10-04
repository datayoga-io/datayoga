---
parent: Blocks
grand_parent: Reference
---

# Remove Fields

Remove fields

**Option 1 (alternative):**
Remove multiple fields

**Properties**

| Name                         | Type       | Description | Required |
| ---------------------------- | ---------- | ----------- | -------- |
| [**fields**](#option1fields) | `object[]` | Fields<br/> | yes      |

**Additional Properties:** not allowed  
**Example**

```yaml
fields:
  - field: credit_card
  - field: name.mname
```

**Option 2 (alternative):**
Remove one field

**Properties**

| Name      | Type     | Description | Required |
| --------- | -------- | ----------- | -------- |
| **field** | `string` | Field<br/>  | yes      |

**Additional Properties:** not allowed  
**Example**

```yaml
field: credit_card
```

<a name="option1fields"></a>

## Option 1: fields\[\]: array

Fields

**Items**

**Item Properties**

| Name      | Type     | Description | Required |
| --------- | -------- | ----------- | -------- |
| **field** | `string` | Field<br/>  | yes      |

**Item Additional Properties:** not allowed  
**Example**

```yaml
- {}
```
