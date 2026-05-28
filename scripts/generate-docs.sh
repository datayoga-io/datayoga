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

# Track temp files so we can clean them up on exit.
RESOLVED_TMP_FILES=()
cleanup_resolved_tmps() {
  for tmp in "${RESOLVED_TMP_FILES[@]}"; do
    [ -f "${tmp}" ] && rm -f "${tmp}"
  done
}
trap cleanup_resolved_tmps EXIT

blocks_dir="./core/src/datayoga_core/blocks"
schemas_dir="./core/src/datayoga_core/resources/schemas"
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
  # Self-contained Python (stdlib only) so this works in CI without installing
  # datayoga_core's runtime dependencies.
  resolved_tmp="$(mktemp --suffix=.schema.json)"
  RESOLVED_TMP_FILES+=("${resolved_tmp}")
  python3 - "${schema}" "${schemas_dir}" > "${resolved_tmp}" <<'PYEOF'
import json
import os
import sys

schema_path, schemas_dir = sys.argv[1], sys.argv[2]
with open(schema_path) as f:
    schema = json.load(f)
inherits = schema.get("$inherit") or []
if inherits:
    if not isinstance(inherits, list) or not all(isinstance(n, str) for n in inherits):
        raise SystemExit(f"$inherit must be a list of strings, got {inherits!r}")
    merged = {}
    for name in inherits:
        fragment_path = os.path.join(schemas_dir, f"{name}.schema.json")
        with open(fragment_path) as f:
            fragment = json.load(f)
        if fragment.get("$inherit"):
            raise SystemExit(f"Nested $inherit in fragment '{name}' is not supported")
        merged.update(fragment.get("properties", {}))
    merged.update(schema.get("properties", {}))
    schema["properties"] = merged
    schema.pop("$inherit", None)
json.dump(schema, sys.stdout)
PYEOF

  npx jsonschema2mk --schema "${resolved_tmp}" --extension yaml-examples \
    --extension front-matter --fm.parent "Blocks" --fm.grand_parent "Reference" > \
    "./docs/reference/blocks/${block_package}${doc_name}"
done

echo "Docs generated successfully"
