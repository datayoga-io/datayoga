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

blocks_dir="./core/src/datayoga_core/blocks"
for schema in $(find ${blocks_dir} -name '*.schema.json' | sort)
do
  doc_name="$(awk -F/ '{ print $(NF-1) }' <<<${schema}).md"
  dir="$(echo ${schema} | sed -e s@${blocks_dir}@@)"
  block_package="$(dirname $(dirname ${dir}))"
  block_package="$(echo ${block_package} | cut -c2- | sed 's/\//_/g')"
  [ ! -z "${block_package}" ] && block_package="${block_package}_"

  npx jsonschema2mk --schema ${schema} --extension yaml-examples \
    --extension front-matter --fm.parent "Blocks" --fm.grand_parent "Reference" > \
    "./docs/reference/blocks/${block_package}${doc_name}"
done

echo "Docs generated successfully"
