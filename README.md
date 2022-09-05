# datayoga-py

## Introduction

`datayoga-py` is the transformation engine used in `DataYoga`, a framework for building and generating data pipelines.

## Installation

```bash
pip install datayoga
```

## Quick Start

This demonstrates how to transform data using a DataYoga job.

### Create a Job

Use this `example.yaml`:

```yaml
steps:
  - uses: add_field
    with:
      - field: full_name
        language: jmespath
        expression: '{ "fname": fname, "lname": lname} | join('' '', values(@))'
      - field: country
        language: sql
        expression: country_code || ' - ' || UPPER(country_name)
  - uses: rename_field
    with:
      - from_field: fname
        to_field: first_name
      - from_field: lname
        to_field: last_name
  - uses: remove_field
    with:
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

### Transform Data Using `datayoga-py`

Use this code snippet to transform a data record using the job defined [above](#create-a-job):

```python
import datayoga as dy
from datayoga.job import Job
from datayoga.utils import read_yaml

job_settings = read_yaml("example.yaml")
job = dy.compile(job_settings)

assert job.transform({"fname": "jane", "lname": "smith", "country_code": 1, "country_name": "usa", "credit_card": "1234-5678-0000-9999", "gender": "F"}) == {"first_name": "jane", "last_name": "smith", "country": "1 - USA", "full_name": "jane smith", "greeting": "Hello Ms. jane smith"}
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

For a full list of supported block types [see reference](https://datayoga-io.github.io/datayoga-py/).

## Expression Language

DataYoga supports both SQL and [JMESPath](https://jmespath.org/) expressions. JMESPath are especially useful to handle nested JSON data, while SQL is more suited to flat row-like structures.

### JMESPath Custom Functions

DataYoga adds the following custom functions to the standard JMESPath library:

| Function     | Description                                                             | Example                                                                                                         | Comments                                                                               |
| ------------ | ----------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| `capitalize` | Capitalizes all the words in the string                                 | Input: `{name: "john doe"}` <br /> Expression: `capitalize(name)` <br /> Output: `John Doe`                     |
| `concat`     | Concatenates an array of variables or literals                          | Input: `{fname: "john", lname: "doe"}` <br /> Expression: `concat([fname,' ',lname])` <br /> Output: `john doe` | This is equivalent to the more verbose built-in expression: `' '.join([fname,lname])`. |
| `lower`      | Converts all uppercase characters in a string into lowercase characters | Input: `{fname: "John"}` <br /> Expression: `lower(fname)` <br /> Output: `john`                                |
| `upper`      | Converts all lowercase characters in a string into uppercase characters | Input: `{fname: "john"}` <br /> Expression: `upper(fname)` <br /> Output: `JOHN`                                |
