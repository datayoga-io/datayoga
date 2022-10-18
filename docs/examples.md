---
nav_order: 8
---

# Examples

## Stream from Kafka topic and load into Postgres

This example reads change events from a Kafka stream and continuously pushes them into Postgres

### Prerequisites

- Set up a local postgres instance and define the connection named `pg` in `connections.yaml`.
- Set up a local Kafka instance and define a topic named 'emp_cdc'
- Define the Kafka connection named `kafka` in `connections.yaml`

### Code

```yaml
#
# read entries from a redis stream named 'emp'
#
- uses: read_kafka
  with:
    connection: cache
    topic: emp
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
        greeting: "'Hello ' || CASE WHEN gender = 'F' THEN 'Ms.' WHEN gender = 'M' THEN 'Mr.' ELSE 'N/A' END || ' ' || full_name",
      }
    language: sql
#
# write records to postgres using APPEND load strategy
#
- uses: write_sql
  with:
    connection: hr
    schema: hr
    table: emp
    load_strategy: APPEND
```

## Read from CSV and load into Redis

This example reads a CSV file from the AirBNB open data, filters by minimum number of reviews, and adds the data into Redis as HASH entries

### Prerequisites

- Set up a local redis instance and define the connection named `redis-data` in `connections.yaml`.
- Download the data set from Kaggle [here](http://data.insideairbnb.com/united-states/ny/new-york-city/2022-09-07/visualisations/listings.csv)

### Code

```yml
#
# read CSV from airbnb
#
- uses: read_csv
  with:
    file: listings.csv
    batch_size: 2500
#
# add a calculated field using SQL expression
#
- uses: add_field
  with:
    field: total_price
    expression: price + `service fee`
    language: sql
#
# customize the key we will use in Redis
#
- uses: add_field
  with:
    field: id
    expression: join(':',['airbnb',"country code",'id',id])
    language: jmespath
- uses: write_redis
  with:
    connection: cache
    command: HSET
    key_field: id
```
