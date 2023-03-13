FROM python:3.8

RUN apt-get update
RUN pip install poetry

RUN echo "#!/usr/local/bin/python" >> /usr/local/bin/datayoga && \
    echo "import sys" >> /usr/local/bin/datayoga && \
    echo "from datayoga.__main__ import main" >> /usr/local/bin/datayoga && \
    echo "if __name__ == '__main__':" >> /usr/local/bin/datayoga && \
    echo "    sys.exit(main())" >> /usr/local/bin/datayoga && \
    echo "" >> /usr/local/bin/datayoga && \
    chmod +x /usr/local/bin/datayoga


RUN mkdir -p /opt/install/cli
RUN mkdir -p /opt/install/core
COPY ./cli/pyproject.toml /opt/install/cli/pyproject.toml
COPY ./cli/poetry.lock /opt/install/cli/poetry.lock
COPY ./core/pyproject.toml /opt/install/core/pyproject.toml
COPY ./core/poetry.lock /opt/install/core/poetry.lock
COPY ./dev/requirements.txt /opt/install/requirements.txt

WORKDIR /opt/install/cli
RUN poetry export -n --without-hashes | grep -v "datayoga" > requirements.txt && \
    pip install -r requirements.txt

#WORKDIR /opt/install/core
#RUN poetry export -E test -n --without-hashes | grep -v "datayoga" | grep -v "pysqlite3-binary" | grep -v "pymssql" > requirements.txt && \
#    pip install -r requirements.txt

WORKDIR /opt/install/core
RUN poetry export -n --without-hashes | grep -v "datayoga" | grep -v "pysqlite3-binary" | grep -v "pymssql" > requirements.txt && \
    pip install -r requirements.txt

WORKDIR /opt/install
RUN pip install -r requirements.txt

ENV PYTHONPATH=/opt/cli/src:/opt/core/src
ENV PYTHONUNBUFFERED=1

RUN mkdir -p /opt/work
WORKDIR /opt/work

STOPSIGNAL SIGKILL
CMD ["tail", "-f", "/dev/null"]
