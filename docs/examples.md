---
nav_order: 9
---

# Examples

## Stream from Kafka topic and load into Postgres

This example reads change events from a Kafka stream and continuously pushes them into Postgres

### Prerequisites

- Set up a local postgres instance and define the connection named `pg` in `connections.dy.yaml`.
- Set up a local Kafka instance and define a topic named 'emp_cdc'
- Define the Kafka connection named `kafka` in `connections.dy.yaml`

### Code

```yaml
steps:
  - uses: read_stream
    with:
      - connection: kafka
        topic: emp_cdc
  - uses: map
    with:
      - expression: $.after
  - uses: relational.write
    with:
      - connection: pg
        table_name: emp
        schema: hr
        load_strategy: APPEND
        key: $.emp_id
```

## Read from CSV and load into Redis

This example reads a CSV file from the AirBNB open data, filters by minimum number of reviews, and adds the data into Redis as HASH entries

### Prerequisites

- Set up a local redis instance and define the connection named `redis-data` in `connections.dy.yaml`.
- Download the data set from Kaggle [here](http://data.insideairbnb.com/united-states/ny/new-york-city/2022-09-07/visualisations/listings.csv)

### Code

```yml
steps:
  - uses: files.read_csv
    with:
      - filename: listings.csv
        delimiter: ,
  - uses: filter
    with:
      - condition: number_of_reviews > 5
  - uses: redis.write
    with:
      - connection: redis-data
        command: HSET
        key: concat('employee:',$.emp_id)
```
