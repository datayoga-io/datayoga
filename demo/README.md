# DataYoga Demo

## Overview

The demo compose file starts the Datayoga `http.receiver` job configured to write incoming 
records into a [PostgreSQL](https://www.postgresql.org) database.
A generator service is used to generate random records and send them to the receiver.

The demo also starts the exporter, [Prometheus](https://prometheus.io), and [Grafana](https://grafana.com) services.

Grafana has the example dashboard that shows the number of records received and processed by the receiver.

## Starting the Demo

Run the following command to start the demo:
```bash 
docker-compose up
```

## Access

### Main Services

- PostgreSQL admin: http://localhost:8082/
- Grafana dashboard: http://localhost:8084/ (Username: `admin@datayoga.io` Password: `datayoga`)

### Additional Services

- Datayoga receiver: http://localhost:8080/
- Datayoga exporter: http://localhost:8081/
- Prometheus dashboard: http://localhost:8083/
- PostgreSQL database: localhost:5432 (Username: `postgres` Password: `datayoga`)

## Stopping the Demo

Run the following command to stop the demo:

```bash
docker-compose down
```
