---
nav_order: 10
---

# Library

## Introduction

`datayoga-py` is the transformation engine used in `DataYoga`, a framework for building and generating data pipelines.

## Installation

```bash
pip install datayoga-py
```

## Quick Start

This demonstrates how to transform data using a DataYoga job.

### Create a Job

Use this `example.yaml`:

```yaml
- uses: add_field
  with:
    field: full_name
    language: jmespath
    expression: '{ "fname": fname, "lname": lname} | join('' '', values(@))'
- uses: rename_field
  with:
    from_field: fname
    to_field: first_name
- uses: rename_field
  with:
    from_field: lname
    to_field: last_name
- uses: remove_field
  with:
    field: credit_card
- uses: add_field
  with:
    field: country
    language: sql
    expression: country_code || ' - ' || UPPER(country_name)
- uses: remove_field
  with:
    field: country_name
- uses: remove_field
  with:
    field: country_code
- uses: map
  with:
    object:
      {
        first_name: first_name,
        last_name: last_name,
        greeting: "'Hello ' || CASE WHEN gender = 'F' THEN 'Ms.' WHEN gender = 'M' THEN 'Mr.' ELSE 'N/A' END || ' ' || full_name",
        country: country,
        full_name: full_name
      }
    language: sql
```

### Transform Data using `datayoga-py`

Use this code snippet to transform a data record using the job defined [above](#create-a-job):

```python
import datayoga_core as dy
from datayoga_core.job import Job
from datayoga_core.utils import read_yaml

job_settings = read_yaml("example.dy.yaml")
job = dy.compile(job_settings)

assert job.transform({"fname": "jane", "lname": "smith", "country_code": 1, "country_name": "usa", "credit_card": "1234-5678-0000-9999", "gender": "F"})[0] == {"first_name": "jane", "last_name": "smith", "country": "1 - USA", "full_name": "jane smith", "greeting": "Hello Ms. jane smith"}
```

As can be seen, the record has been transformed based on the job:

- `fname` field renamed to `first_name`.
- `lname` field renamed to `last_name`.
- `country` field added based on an SQL expression.
- `full_name` field added based on a [JMESPath](https://jmespath.org/) expression.
- `greeting` field added based on an SQL expression.

## Block Reference

For a full list of supported block types [see reference](blocks.md).

### Examples

- Add a new field `country` out of an SQL expression that concatenates `country_code` and `country_name` fields after upper case the later:

  ```yaml
  uses: add_field
  with:
    field: country
    language: sql
    expression: country_code || ' - ' || UPPER(country_name)
  ```

- Rename field `lname` to `last_name`:

  ```yaml
  uses: rename_field
  with:
    from_field: lname
    to_field: last_name
  ```

- Remove `credit_card` field:

  ```yaml
  uses: remove_field
  with:
    field: credit_card
  ```
