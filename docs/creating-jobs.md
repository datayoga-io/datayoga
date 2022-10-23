---
nav_order: 5
---

# Creating Jobs

Jobs are created by creating yaml files in the `jobs` folder. Each job is composed of several `steps` that activate `blocks`. A `block` defines the business logic of an action. For example, a `block` can write to a Kafka stream, can read from a cloud API, can transform structure, or enrich a message with external data. A `steps` activates the `block` with a set of parameters.

## Overview of the Job Yaml Structure

Each Job must start with a block that either produces data or accepts data from external sources.
The subsequent blocks each receive the output of the previous step as an input. The data will be streamed through these blocks as data flows through the chain.

## Launch the DataYoga Runner

The DataYoga Runner is a processing engine that runs Jobs. Jobs can either be Batch or Streaming, depending on the type of input block that is used as part of the Job.
It provides:

- Validation
- Error handling
- Metrics and observability
- Credentials management

The Runtime supports multiple stream [processing strategies](processing-strategies.md) including:

- Stream processing
- Parallelism
- Buffering
- Rate limit

It supports both async processing, multi-threading, and multi-processing to enable maximum throughput with a low footprint.

To deploy a job to the DataYoga Runner, use the DataYoga CLI.

```bash
datayoga run jobname.yaml
```

## Tutorial - a Job that Reads from Redis and Writes to Postgres

### Set up Environment

For the purpose of the tutorial, set up two containers.

- A Redis source:

```bash
docker run -p 6379:6379 redis
```

- A Postgres target:

```bash
docker run -p 5432:5432 -e POSTGRES_PASSWORD=mysecretpassword -d postgres
```

### Define Connections

DataYoga manages connections in a special file named `connections.yaml`. Each connection is defined with a logical name and can define properties needed for the connection. Reference to environment variables, interpolation, and secrets is available.

Add the connections to Redis and Postgres above to the connections.yaml:

```bash
cat << EOF > connections.yaml
- name: hr
  type: postgresql
  host: localhost
  port: 5432
  database: postgres
  user: postgres
  password: postgres
- name: stream
  type: redis
  host: localhost
  port: 6379
EOF
```

### Create the Job

```bash
cat << EOF > redis_to_pg.yaml
steps:
- uses: redis.read_stream
  with:
    connection: stream
    stream_name: emp
- uses: add_field
  with:
    fields:
      - field: full_name
        language: jmespath
        expression: concat([fname, ' ' , lname])
- uses: map
  with:
    expression:
      {
        first_name: fname,
        last_name: lname,
        country: country_code || ' - ' || UPPER(country_name),
        full_name: full_name,
        greeting: "'Hello ' || CASE WHEN gender = 'F' THEN 'Ms.' WHEN gender = 'M' THEN 'Mr.' ELSE 'N/A' END || ' ' || full_name"
      }
    language: sql
- uses: relational.write
  with:
    connection: hr
    schema: public
    table: emp
    create: true
EOF
```

### Run the Job in the DataYoga Runner

```bash
datayoga run redis_to_pg.yaml
```
