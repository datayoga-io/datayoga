from prometheus_client import Counter, start_http_server

incoming_records = Counter("incoming_records", "Number of incoming records")
processed_entries = Counter("processed_records", "Number of processed records", ("step",))
rejected_records = Counter("rejected_records", "Number of rejected records", ("step",))
filtered_records = Counter("filtered_records", "Number of filtered records", ("step",))


def start(port: int):
    """Starts the prometheus metrics exporter on selected port."""

    start_http_server(port)
