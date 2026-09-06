import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import test from 'node:test';

const auditHook = new URL('../hooks/audit-bash-result.mjs', import.meta.url);

test('shipped plugin discovers the default guard and both audit hooks', () => {
  const packageRoot = path.dirname(new URL('../package.json', import.meta.url).pathname);
  const hooks = JSON.parse(fs.readFileSync(path.join(packageRoot, 'hooks', 'hooks.json'), 'utf8'));
  assert.deepEqual(Object.keys(hooks.hooks).sort(), [
    'PostToolUse',
    'PostToolUseFailure',
    'PreToolUse',
  ]);
  for (const event of Object.values(hooks.hooks)) {
    assert.ok(event.length >= 1);
    for (const entry of event) {
      const script = entry.hooks[0].args[0].replace('${ZCODE_PLUGIN_ROOT}/', '');
      assert.equal(fs.existsSync(path.join(packageRoot, script)), true, script);
    }
  }
  const manifest = JSON.parse(fs.readFileSync(path.join(packageRoot, '.codex-plugin', 'plugin.json'), 'utf8'));
  assert.equal(manifest.hooks, './hooks/hooks.json');
  assert.equal(fs.existsSync(path.resolve(packageRoot, manifest.hookPolicy.filePolicy)), true);
  assert.equal(manifest.hookPolicy.postToolUseAudit, true);
});

test('default PostToolUse audit writes bounded metadata without raw output', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-hook-audit-root-'));
  const data = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-hook-audit-data-'));
  fs.writeFileSync(path.join(root, 'README.md'), 'needle\n');
  const input = {
    session_id: 'session-audit',
    tool_use_id: 'tool-audit',
    hook_event_name: 'PostToolUse',
    tool_name: 'Bash',
    cwd: root,
    duration_ms: 17,
    tool_input: { command: 'cat README.md' },
    tool_response: { status_code: 0, stdout: 'sensitive output', stderr: '' },
  };
  const proc = spawnSync(process.execPath, [auditHook.pathname], {
    input: `${JSON.stringify(input)}\n`,
    encoding: 'utf8',
    env: { ...process.env, ZCODE_PLUGIN_DATA: data },
  });
  assert.equal(proc.status, 0, proc.stderr);
  const raw = fs.readFileSync(path.join(data, 'readonly-bash-audit.jsonl'), 'utf8');
  const record = JSON.parse(raw.trim());
  assert.equal(record.tool_use_id, 'tool-audit');
  assert.equal(record.status_code, 0);
  assert.equal(record.duration_ms, 17);
  assert.equal('policy_decision' in record, false);
  assert.match(record.stdout_sha256, /^[a-f0-9]{64}$/u);
  assert.equal(raw.includes('sensitive output'), false);
});
