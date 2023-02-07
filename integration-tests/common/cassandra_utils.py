import time
from typing import List

from cassandra.cluster import Cluster, Session
from testcontainers.core.container import DockerContainer


def get_cassandra_container() -> DockerContainer:
    return DockerContainer(image="rinscy/cassandra").\
        with_bind_ports(9042, 9042)


def get_cassandra_session(hosts: List[str], timeout=60000) -> Session:
    start_time = time.time() * 1000
    while True:
        if time.time() * 1000 - start_time > timeout:
            raise ValueError("failed to connect to Cassandra cluster")

        try:
            cluster = Cluster(hosts)
            return cluster.connect()
        except Exception:
            time.sleep(0.5)


def create_keyspace(session: Session, keyspace: str):
    session.execute(f"CREATE KEYSPACE {keyspace} WITH replication = "
                    "{'class': 'SimpleStrategy', 'replication_factor': '1'};")


def create_emp_table(session: Session, keyspace: str):
    session.execute(
        f"CREATE TABLE {keyspace}.emp (id int, full_name text, country text, address text, gender text, PRIMARY KEY (id));")


def insert_to_emp_table(session: Session, keyspace: str):
    session.execute(
        f"INSERT INTO {keyspace}.emp (id, full_name, country, gender) VALUES (1, 'John Doe', '972 - ISRAEL', 'M')")
    session.execute(
        f"INSERT INTO {keyspace}.emp (id, full_name, country, gender) VALUES (10, 'john doe', '972 - ISRAEL', 'M')")
    session.execute(
        f"INSERT INTO {keyspace}.emp (id, full_name, country, gender, address) VALUES (12, 'steve steve', '972 - ISRAEL', 'M', 'main street')")
