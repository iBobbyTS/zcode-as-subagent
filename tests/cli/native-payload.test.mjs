import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';

const root = path.resolve(import.meta.dirname, '../..');
const binary = path.join(root, 'npm/native/darwin-arm64/zcode-as-subagentd');
const facade = path.join(root, 'npm/native/darwin-arm64/zcode-as-subagent-mcp');

test('macOS arm64 daemon payload is present and executable', () => {
  const stat = fs.statSync(binary);
  assert.equal(stat.mode & 0o777, 0o755);
  assert.ok(stat.size > 0);
  assert.equal(fs.statSync(facade).mode & 0o777, 0o755);
  assert.equal(fs.existsSync(path.join(root, 'npm/native/darwin-arm64/zcode-agentd')), false);
  assert.equal(fs.existsSync(path.join(root, 'npm/native/darwin-arm64/zcode-subagent-mcp')), false);
});

test('npm files whitelist includes the native payload directory', () => {
  const packageJson = JSON.parse(fs.readFileSync(path.join(root, 'package.json'), 'utf8'));
  assert.ok(packageJson.files.includes('npm/'));
  assert.deepEqual(packageJson.bin, {
    zas: 'bin/zas.mjs',
    'zcode-as-subagent-mcp': 'bin/zcode-as-subagent-mcp.mjs',
  });
});
