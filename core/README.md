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

Use this `example.yaml`:

```yaml
- steps:
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
            full_name: full_name,
          }
        language: sql
```

### Transform Data Using `datayoga-core`

Use this code snippet to transform a data record using the job defined [above](#create-a-job):

```python
import datayoga_core as dy
from datayoga_core.job import Job
from datayoga_core.utils import read_yaml

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

For a full list of supported block types [see reference](https://datayoga-io.github.io/datayoga-py/).

## Expression Language

DataYoga supports both SQL and [JMESPath](https://jmespath.org/) expressions. JMESPath are especially useful to handle nested JSON data, while SQL is more suited to flat row-like structures.

### Notes

- Dot notation in expression represents nesting fields in the object, for example `name.first_name` refers to `{ "name": { "first_name": "John" } }`.
- In order to refer to a field that contains a dot in its name, escape it, for example `name\.first_name` refers to `{ "name.first_name": "John" }`.

### JMESPath Custom Functions

DataYoga adds the following custom functions to the standard JMESPath library:

| Function             | Description                                                                                                                         | Example                                                                                                                                           | Comments                                                                                                                                                         |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `capitalize`         | Capitalizes all the words in the string                                                                                             | Input: `{"name": "john doe"}` <br /> Expression: `capitalize(name)` <br /> Output: `John Doe`                                                     |                                                                                                                                                                  |
| `concat`             | Concatenates an array of variables or literals                                                                                      | Input: `{"fname": "john", "lname": "doe"}` <br /> Expression: `concat([fname, ' ' ,lname])` <br /> Output: `john doe`                             | This is equivalent to the more verbose built-in expression: `' '.join([fname,lname])`                                                                            |
| `hash`               | Calculates a hash using the `hash_name` hash function and returns its hexadecimal representation                                    | Input: `{"some_str": "some_value"}` <br /> Expression: `hash(some_str, &#96;sha1&#96;)` <br /> Output: `8c818171573b03feeae08b0b4ffeb6999e3afc05` | Supported algorithms: sha1 (default), sha256, md5, sha384, sha3_384, blake2b, sha512, sha3_224, sha224, sha3_256, sha3_512, blake2s                              |
| `left`               | Returns a specified number of characters from the start of a given text string                                                      | Input: `{"greeting": "hello world!"}` <br /> Expression: `` left(greeting, `5`) `` <br /> Output: `hello`                                         |                                                                                                                                                                  |
| `lower`              | Converts all uppercase characters in a string into lowercase characters                                                             | Input: `{"fname": "John"}` <br /> Expression: `lower(fname)` <br /> Output: `john`                                                                |                                                                                                                                                                  |
| `mid `               | Returns a specified number of characters from the middle of a given text string                                                     | Input: `{"greeting": "hello world!"}` <br /> Expression: `` mid(greeting, `4`, `3`) `` <br /> Output: `o w`                                       |                                                                                                                                                                  |
| `replace`            | Replaces all the occurrences of a substring with a new one                                                                          | Input: `{"sentence": "one four three four!"}` <br /> Expression: `replace(sentence, 'four', 'two')` <br /> Output: `one two three two!`           |                                                                                                                                                                  |
| `right`              | Returns a specified number of characters from the end of a given text string                                                        | Input: `{"greeting": "hello world!"}` <br /> Expression: `` right(greeting, `6`) `` <br /> Output: `world!`                                       |                                                                                                                                                                  |
| `split`              | Splits a string into a list of strings after breaking the given string by the specified delimiter (comma by default)                | Input: `{"departments": "finance,hr,r&d"}` <br /> Expression: `split(departments)` <br /> Output: `['finance', 'hr', 'r&d']`                      | Default delimiter is comma - a different delimiter can be passed to the function as the second argument, for example: `split(departments, ';')`                  |
| `time_delta_days`    | Returns the number of days between a given `dt` and now (positive) or the number of days that have passed from now (negative)       | Input: `{"dt": '2021-10-06T18:56:16.701670+00:00'}` <br /> Expression: `time_delta_days(dt)` <br /> Output: `365`                                 | If `dt` is a string, ISO datetime (2011-11-04T00:05:23+04:00, for example) is assumed. If `dt` is a number, Unix timestamp (1320365123, for example) is assumed. |
| `time_delta_seconds` | Returns the number of seconds between a given `dt` and now (positive) or the number of seconds that have passed from now (negative) | Input: `{"dt": '2021-10-06T18:56:16.701670+00:00'}` <br /> Expression: `time_delta_days(dt)` <br /> Output: `31557600`                            | If `dt` is a string, ISO datetime (2011-11-04T00:05:23+04:00, for example) is assumed. If `dt` is a number, Unix timestamp (1320365123, for example) is assumed. |
| `upper`              | Converts all lowercase characters in a string into uppercase characters                                                             | Input: `{"fname": "john"}` <br /> Expression: `upper(fname)` <br /> Output: `JOHN`                                                                |                                                                                                                                                                  |
| `uuid`               | Generates a random UUID4 and returns it as a string in standard format                                                              | Input: None <br /> Expression: `uuid()` <br /> Output: `3264b35c-ff5d-44a8-8bc7-9be409dac2b7`                                                     |                                                                                                                                                                  |
