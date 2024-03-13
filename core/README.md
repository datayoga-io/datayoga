# DataYoga Core

## Introduction

`datayoga-core` is the transformation engine used in `DataYoga`, a framework for building and generating data pipelines.

## Installation

```bash
pip install datayoga-core
```

## Quick Start

This demonstrates how to transform data using a DataYoga job.

### Create a Job

Use this `example.dy.yaml`:

```yaml
steps:
  - uses: add_field
    with:
      fields:
        - field: full_name
          language: jmespath
          expression: concat([fname, ' ' , lname])
        - field: country
          language: sql
          expression: country_code || ' - ' || UPPER(country_name)
  - uses: rename_field
    with:
      fields:
        - from_field: fname
          to_field: first_name
        - from_field: lname
          to_field: last_name
  - uses: remove_field
    with:
      fields:
        - field: credit_card
        - field: country_name
        - field: country_code
  - uses: map
    with:
      expression:
        {
          first_name: first_name,
          last_name: last_name,
          greeting: "'Hello ' || CASE WHEN gender = 'F' THEN 'Ms.' WHEN gender = 'M' THEN 'Mr.' ELSE 'N/A' END || ' ' || full_name",
          country: country,
          full_name: full_name
        }
      language: sql
```

### Transform Data Using `datayoga-core`

Use this code snippet to transform a data record using the job defined [above](#create-a-job). The transform method returns a tuple of processed, filtered, and rejected records:

```python
import datayoga_core as dy
from datayoga_core.job import Job
from datayoga_core.result import Result, Status
from datayoga_core.utils import read_yaml

job_settings = read_yaml("example.dy.yaml")
job = dy.compile(job_settings)

assert job.transform([{"fname": "jane", "lname": "smith", "country_code": 1, "country_name": "usa", "credit_card": "1234-5678-0000-9999", "gender": "F"}]).processed == [
  Result(status=Status.SUCCESS, payload={"first_name": "jane", "last_name": "smith", "country": "1 - USA", "full_name": "jane smith", "greeting": "Hello Ms. jane smith"})]
```

The job can also be provided as a parsed json inline:

```python
import datayoga_core as dy
from datayoga_core.job import Job
from datayoga_core.result import Result, Status
import yaml
import textwrap

job_settings = textwrap.dedent("""
  steps:
    - uses: add_field
      with:
        fields:
          - field: full_name
            language: jmespath
            expression: concat([fname, ' ' , lname])
          - field: country
            language: sql
            expression: country_code || ' - ' || UPPER(country_name)
    - uses: rename_field
      with:
        fields:
          - from_field: fname
            to_field: first_name
          - from_field: lname
            to_field: last_name
    - uses: remove_field
      with:
        fields:
          - field: credit_card
          - field: country_name
          - field: country_code
    - uses: map
      with:
        expression:
          {
            first_name: first_name,
            last_name: last_name,
            greeting: "'Hello ' || CASE WHEN gender = 'F' THEN 'Ms.' WHEN gender = 'M' THEN 'Mr.' ELSE 'N/A' END || ' ' || full_name",
            country: country,
            full_name: full_name
          }
        language: sql
""")
job = dy.compile(yaml.safe_load(job_settings))

assert job.transform([{"fname": "jane", "lname": "smith", "country_code": 1, "country_name": "usa", "credit_card": "1234-5678-0000-9999", "gender": "F"}]).processed == [
  Result(status=Status.SUCCESS, payload={"first_name": "jane", "last_name": "smith", "country": "1 - USA", "full_name": "jane smith", "greeting": "Hello Ms. jane smith"})]
```

As can be seen, the record has been transformed based on the job:

- `fname` field renamed to `first_name`.
- `lname` field renamed to `last_name`.
- `country` field added based on an SQL expression.
- `full_name` field added based on a [JMESPath](https://jmespath.org/) expression.
- `greeting` field added based on an SQL expression.

### Examples

- Add a new field `country` out of an SQL expression that concatenates `country_code` and `country_name` fields after upper case the later:

  ```yaml
  uses: add_field
  with:
    field: country
    language: sql
    expression: country_code || ' - ' || UPPER(country_name)
  ```

- Rename `fname` field to `first_name` and `lname` field to `last_name`:

  ```yaml
  uses: rename_field
  with:
    fields:
      - from_field: fname
        to_field: first_name
      - from_field: lname
        to_field: last_name
  ```

- Remove `credit_card` field:

  ```yaml
  uses: remove_field
  with:
    field: credit_card
  ```

For a full list of supported block types [see reference](https://datayoga-io.github.io/library).

## Expression Language

DataYoga supports both SQL and [JMESPath](https://jmespath.org/) expressions. JMESPath are especially useful to handle nested JSON data, while SQL is more suited to flat row-like structures.

For more information about custom functions and supported expression language syntax [see reference](https://datayoga-io.github.io/expressions).
