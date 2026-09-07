#!/bin/sh
set -eu

evidence_dir=.agent-work/evidence/npm-tarball
if [ "$#" -gt 1 ]; then
  echo "usage: $0 [package.tgz]" >&2
  exit 2
fi
if [ "$#" -eq 1 ]; then
  package_file=$1
else
  package_file=$(find "$evidence_dir" -maxdepth 1 -name 'zcode-as-subagent-*.tgz' -type f \
    -exec stat -f '%m %N' {} \; | sort -k1,1nr -k2,2 | head -1 | cut -d' ' -f2-)
fi
test -n "$package_file"
test -f "$package_file"
test -r "$package_file"
case "$package_file" in
  *.tgz) ;;
  *) echo "package must be a .tgz file: $package_file" >&2; exit 2 ;;
esac
mkdir -p "$evidence_dir"

tar -tzf "$package_file" > "$evidence_dir/contents.txt"
grep -qx 'package/npm/native/darwin-arm64/zcode-as-subagentd' .agent-work/evidence/npm-tarball/contents.txt
grep -qx 'package/npm/native/darwin-arm64/zcode-as-subagent-mcp' .agent-work/evidence/npm-tarball/contents.txt
if grep -Eq '(^|/)(\.agent-work|workspace|target|node_modules|\.npm|.*\.sqlite3|.*\.log|.*credentials|.*runtime)' .agent-work/evidence/npm-tarball/contents.txt; then
  echo 'forbidden release material found' >&2
  exit 1
fi

for native_file in \
  npm/native/darwin-arm64/zcode-as-subagentd \
  npm/native/darwin-arm64/zcode-as-subagent-mcp; do
  test -x "$native_file"
  file "$native_file" | grep -q 'Mach-O 64-bit executable arm64'
  test "$(stat -f '%Lp' "$native_file")" = 755
done
shasum -a 256 npm/native/darwin-arm64/zcode-as-subagentd npm/native/darwin-arm64/zcode-as-subagent-mcp "$package_file" > "$evidence_dir/sha256sums.txt"
printf '%s\n' 'native tarball static checks passed'
