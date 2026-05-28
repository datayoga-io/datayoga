#!/bin/bash
set -e

cd "$(dirname "$0")/.."

# Generate resource schema doc
cp ./docs/reference/blocks.md /tmp/blocks.md
cp ./docs/reference/connection_types.md /tmp/connection_types.md

rm -rf ./docs/reference
mkdir ./docs/reference

nav_order=1
for schema in ./core/src/datayoga_core/resources/schemas/*.schema.json
do
  npx jsonschema2mk --schema ${schema} --extension yaml-examples \
    --extension front-matter --fm.parent "Reference" --fm.nav_order "${nav_order}" > \
  ./docs/reference/$(basename "${schema}" .schema.json).md
  nav_order=$((nav_order+1))
done

cp /tmp/blocks.md ./docs/reference/blocks.md
cp /tmp/connection_types.md ./docs/reference/connection_types.md

# Generate connection type schema docs
rm -rf ./docs/reference/connection_types
mkdir ./docs/reference/connection_types

for schema in ./core/src/datayoga_core/resources/schemas/connections/*.schema.json
do
  npx jsonschema2mk --schema ${schema} --extension yaml-examples \
    --extension front-matter --fm.parent "Connection Types" --fm.grand_parent "Reference" > \
    ./docs/reference/connection_types/$(basename "${schema}" .schema.json).md
done

# Generate block schema docs
rm -rf ./docs/reference/blocks
mkdir ./docs/reference/blocks

# Pick a Python that can import datayoga_core via PYTHONPATH=core/src.
if [ -x "./core/.venv/bin/python" ]; then
  DOC_PYTHON="./core/.venv/bin/python"
elif [ -x "./venv/bin/python" ]; then
  DOC_PYTHON="./venv/bin/python"
else
  DOC_PYTHON="python3"
fi

# Track temp files so we can clean them up on exit.
RESOLVED_TMP_FILES=()
cleanup_resolved_tmps() {
  for tmp in "${RESOLVED_TMP_FILES[@]}"; do
    [ -f "${tmp}" ] && rm -f "${tmp}"
  done
}
trap cleanup_resolved_tmps EXIT

blocks_dir="./core/src/datayoga_core/blocks"
for schema in $(find ${blocks_dir} -name '*.schema.json' | sort)
do
  doc_name="$(awk -F/ '{ print $(NF-1) }' <<<${schema}).md"
  dir="$(echo ${schema} | sed -e s@${blocks_dir}@@)"
  block_package="$(dirname $(dirname ${dir}))"
  block_package="$(echo ${block_package} | cut -c2- | sed 's/\//_/g')"
  [ ! -z "${block_package}" ] && block_package="${block_package}_"

  # Resolve $inherit fragments so jsonschema2mk sees the inherited properties
  # (batch_size, flush_ms, etc.). jsonschema2mk does not understand our custom
  # $inherit extension, so we materialize a resolved copy first.
  resolved_tmp="$(mktemp --suffix=.schema.json)"
  RESOLVED_TMP_FILES+=("${resolved_tmp}")
  PYTHONPATH=core/src "${DOC_PYTHON}" -c "
import json, sys
from datayoga_core.schema_utils import resolve_inherits
from datayoga_core import utils
schema = utils.read_json('${schema}')
resolved = resolve_inherits(schema)
sys.stdout.write(json.dumps(resolved))
" > "${resolved_tmp}"

  npx jsonschema2mk --schema "${resolved_tmp}" --extension yaml-examples \
    --extension front-matter --fm.parent "Blocks" --fm.grand_parent "Reference" > \
    "./docs/reference/blocks/${block_package}${doc_name}"
done

echo "Docs generated successfully"
