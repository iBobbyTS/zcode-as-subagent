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
prefix=$(mktemp -d)
test_home=$(mktemp -d)
trap 'rm -rf "$prefix" "$test_home"' EXIT INT TERM

HOME="$test_home" npm install --global --prefix "$prefix" "$package_file" > "$evidence_dir/install.log"
HOME="$test_home" "$prefix/bin/zcode-as-subagent" status > "$evidence_dir/status.json"
HOME="$test_home" "$prefix/bin/zcode-as-subagent" init --dry-run > "$evidence_dir/dry-run.json"
HOME="$test_home" "$prefix/bin/zcode-as-subagent" init > "$evidence_dir/init.json"

daemon="$prefix/lib/node_modules/zcode-as-subagent/npm/native/darwin-arm64/zcode-as-subagentd"
facade="$prefix/lib/node_modules/zcode-as-subagent/npm/native/darwin-arm64/zcode-as-subagent-mcp"
plist="$test_home/Library/LaunchAgents/com.zcode-as-subagent.daemon.plist"
test -x "$daemon"
test -x "$facade"
test -f "$plist"
grep -Fq "$daemon" "$plist"
grep -Fq '/Applications/ZCode.app/Contents/Resources/glm/zcode.cjs' "$plist"
printf '%s\n' 'installed tarball checks passed'
