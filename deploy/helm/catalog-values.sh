#!/bin/sh
# Render a catalogue directory as Helm values for the statusd chart (catalog.files).
#   deploy/helm/catalog-values.sh catalog/ > catalog-values.yaml
# Validate the catalogue first: statusd -config <cfg> -check
set -eu
dir=${1:?usage: $0 <catalog-dir>}
found=0
printf 'catalog:\n  files:\n'
for f in "$dir"/*.yml "$dir"/*.yaml; do
  [ -f "$f" ] || continue
  found=1
  printf '    %s: |\n' "$(basename "$f")"
  sed 's/^/      /' "$f"
  # A trailing newline keeps the block scalar intact when the file lacks one.
  [ -z "$(tail -c1 "$f")" ] || printf '\n'
done
[ "$found" = 1 ] || { echo "no *.yml files in $dir" >&2; exit 1; }
