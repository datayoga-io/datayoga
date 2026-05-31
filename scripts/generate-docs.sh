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
for schema in $(find ${blocks_dir} -name '*.schema.json' | sort)
do
  doc_name="$(awk -F/ '{ print $(NF-1) }' <<<${schema}).md"
  dir="$(echo ${schema} | sed -e s@${blocks_dir}@@)"
  block_package="$(dirname $(dirname ${dir}))"
  block_package="$(echo ${block_package} | cut -c2- | sed 's/\//_/g')"
  [ ! -z "${block_package}" ] && block_package="${block_package}_"

  # Materialize a docs-friendly copy of the schema:
  #   1. Resolve local-file $ref nodes by inlining the referenced JSON.
  #   2. Flatten allOf-contributed properties into the top-level `properties`
  #      so jsonschema2mk renders a single property table per block.
  # Self-contained Python (stdlib only) so this works in CI without installing
  # datayoga_core's runtime dependencies. Pre-resolve at doc-gen time only;
  # the on-disk schemas remain standard JSON Schema.
  resolved_tmp="$(mktemp --suffix=.schema.json)"
  RESOLVED_TMP_FILES+=("${resolved_tmp}")
  python3 - "${schema}" > "${resolved_tmp}" <<'PYEOF'
import json
import os
import sys


def resolve_node(node, base_dir, visited):
    if isinstance(node, dict):
        ref = node.get("$ref")
        if isinstance(ref, str) and not ref.startswith("#") and "://" not in ref and ref.endswith(".json"):
            target = os.path.normpath(os.path.join(base_dir, ref))
            if target in visited:
                raise SystemExit(f"Circular $ref at {target}")
            if not os.path.isfile(target):
                raise SystemExit(f"$ref target not found: {ref} -> {target}")
            with open(target) as f:
                fragment = json.load(f)
            visited.add(target)
            try:
                return resolve_node(fragment, os.path.dirname(target), visited)
            finally:
                visited.discard(target)
        return {k: resolve_node(v, base_dir, visited) for k, v in node.items()}
    if isinstance(node, list):
        return [resolve_node(item, base_dir, visited) for item in node]
    return node


def flatten_allof_properties(schema):
    """Inline `allOf[*].properties` into the top-level `properties`, removing
    the allOf. Docs-only transformation so jsonschema2mk renders one table."""
    if not isinstance(schema, dict) or "allOf" not in schema:
        return schema
    merged = {}
    for member in schema.get("allOf", []):
        if isinstance(member, dict):
            merged.update(member.get("properties", {}))
    merged.update(schema.get("properties", {}))
    schema["properties"] = merged
    schema.pop("allOf", None)
    return schema


schema_path = sys.argv[1]
with open(schema_path) as f:
    schema = json.load(f)
schema = resolve_node(schema, os.path.dirname(os.path.abspath(schema_path)), set())
schema = flatten_allof_properties(schema)
json.dump(schema, sys.stdout)
PYEOF

  npx jsonschema2mk --schema "${resolved_tmp}" --extension yaml-examples \
    --extension front-matter --fm.parent "Blocks" --fm.grand_parent "Reference" > \
    "./docs/reference/blocks/${block_package}${doc_name}"
done

echo "Docs generated successfully"
