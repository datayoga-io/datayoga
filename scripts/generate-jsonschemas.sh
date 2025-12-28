#!/bin/bash
set -e

cd "$(dirname "$0")/.."

# Ensure deterministic ordering in Python dicts/sets
export PYTHONHASHSEED=0

# Build and install DataYoga Core
cd core
poetry build
pip install dist/*.whl --force-reinstall -q
cd ..

# Generate DataYoga Job and Connections Schemas
python -c "import json; from datayoga_core.job import Job; schema = Job.get_json_schema(); open('schemas/job.schema.json', 'w').write(json.dumps(schema))"
python -c "import json; from datayoga_core.connection import Connection; schema = Connection.get_json_schema(); open('schemas/connections.schema.json', 'w').write(json.dumps(schema))"

# Prettify JSON Schema files
prettier --write "schemas/**/*.json"

echo "JSON schemas generated successfully"
