#!/usr/bin/env bash
# Rebuild docs/diagrams/*.png and *.svg from the Archify sources in docs/diagrams/src.
#
# Needs: node >= 18, the Archify skill (npx skills add tt-a1i/archify -g), a Chromium
# binary (ARCHIFY_CHROME, or chrome/chromium on PATH) and playwright-core for the export
# step (npm install --no-save playwright-core).
set -euo pipefail
cd "$(dirname "$0")/.."

ARCHIFY=${ARCHIFY:-$HOME/.agents/skills/archify/bin/archify.mjs}
export ARCHIFY_CHROME_NO_SANDBOX=${ARCHIFY_CHROME_NO_SANDBOX:-1}
mkdir -p .archify

names=()
for src in docs/diagrams/src/*.json; do
  base=$(basename "$src")
  name=${base%%.*}
  type=$(echo "$base" | cut -d. -f2)
  echo "== $name ($type)"
  # finalize = validate + deliver + strict check + real-browser check; non-zero exit is a failure.
  node "$ARCHIFY" finalize "$type" "$src" ".archify/$name.html" \
    --quality showcase --out-dir ".archify/receipts/$name"
  names+=("$name")
done

node scripts/diagrams-export.mjs .archify docs/diagrams "${names[@]}"
