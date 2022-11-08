---
nav_order: 2
---

# Getting Started

## Upgrade [Pip](https://pypi.org/project/pip/)

```bash
pip install --upgrade pip
```

## Install DataYoga CLI

```bash
pip install datayoga
```

Verify that the installation completed successfully by running this command:

```bash
datayoga --version
```

## Create New DataYoga Project

To create a new DataYoga project, use the `init` command:

```bash
datayoga init hello_world
cd hello_world
```

> [Directory structure](directory-structure.md)

## Run Your First Job

Let's run our first job. It is pre-defined in the samples folder as part of the `init` command. Our job is going to load a CSV file from the samples folder and transform it into JSON:

```bash
datayoga run sample.hello
```

If all goes well, you should see a list of JSON values that correspond to each line in the input csv file:

```yaml
{"id": "1", "fname": "john", "lname": "doe", "credit_card": "1234-1234-1234-1234", "country_code": "972", "country_name": "israel", "gender": "M", "full_name": "John Doe", "greeting": "HelMr. John Doe"}
{"id": "2", "fname": "jane", "lname": "doe", "credit_card": "1000-2000-3000-4000", "country_code": "972", "country_name": "israel", "gender": "F", "full_name": "Jane Doe", "greeting": "HelMs. Jane Doe"}
{"id": "3", "fname": "bill", "lname": "adams", "credit_card": "9999-8888-7777-666", "country_code": "1", "country_name": "usa", "gender": "M", "full_name": "Bill Adams", "greeting": "HelMr. Bill Adams"}
```

That's it! You've created your first job that loads data from CSV, runs it through a series of transformation steps, and shows the data to the standard output. A good start!

## Enhance The Job Definition

Let's customize the structure of the resulting JSON.
The jobs are located under the `jobs` folder and can be arrange into modules. The `sample.hello` job you just ran is located in `jobs/sample/hello.yaml`.

Add a new step of type `map` into the chain of steps. To do this, open the job definition in a text editor and add the section highlighted in bold to the job definition:

<pre><code>
input:
  uses: files.read_csv
  with:
    file: sample.csv
steps:
  - uses: add_field
    with:
      fields:
        - field: full_name
          language: jmespath
          expression: concat([capitalize(fname), ' ' , capitalize(lname)])
        - field: greeting
          language: sql
          expression: "'Hello ' || CASE WHEN gender = 'F' THEN 'Ms.' WHEN gender = 'M' THEN 'Mr.' ELSE 'N/A' END || ' ' || full_name"
<b>
  - uses: map
    with:
      expression: |
        { greeting: greeting,
          details: {
              id: id,
              first_name: fname,
              last_name: lname
          }
        }
      language: jmespath
</b>
  - uses: std.write
</code>
</pre>

Run the job again:

```bash
datayoga run sample.hello
```

You should now see the modified JSON as the output:

```bash
{"greeting": "HelMr. John Doe", "details": {"first_name": "john", "last_name": "doe"}}
{"greeting": "HelMs. Jane Doe", "details": {"first_name": "jane", "last_name": "doe"}}
{"greeting": "HelMr. Bill Adams", "details": {"first_name": "bill", "last_name": "adams"}}
```

Read on for a more detailed tutorial or check out the [reference](reference/blocks.md) to see the different block types currently available.
