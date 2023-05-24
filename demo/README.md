# DataYoga Demo

## Overview

The demo compose file starts the Datayoga `http.receiver` job configured to write incoming records into PostgreSQL database. 
Generator service is used to generate random records and send them to the receiver.

The demo also starts the exporter, prometheus, and grafana services.

Grafana have the example dashboard that shows the number of records received and processed by the receiver.

## Start the Demo

Run the following command to start the demo:

```bash 
docker-compose up
```

## Access

### Main services

* PostgreSQL admin: http://localhost:8082
* Grafana dashboard: http://localhost:8084

### Additional services

* Datayoga receiver: http://localhost:8080
* Datayoga exporter: http://localhost:8081
* Prometheus dashboard: http://localhost:8083
* Postgres database: localhost:5432

## Stop the Demo

Run the following command to stop the demo:

```bash
docker-compose down
```
