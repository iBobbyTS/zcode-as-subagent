#!/usr/bin/env node
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

const pluginRoot = path.resolve(new URL('..', import.meta.url).pathname);
const configPath = process.argv[process.argv.indexOf('--config') + 1];
const provenanceIndex = process.argv.indexOf('--provenance');
const provenancePath = provenanceIndex >= 0 ? process.argv[provenanceIndex + 1] : path.join(path.dirname(configPath ?? ''), 'zcode-agent-hook-provenance.json');
if (!configPath || configPath.startsWith('--')) process.exit(2);
const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
const events = config?.hooks?.events ?? {};
assert.equal(config?.hooks?.enabled, true, 'hooks are disabled');
const expectedScripts = {
  PreToolUse: [['^(Read|Grep|Glob|Write|Edit|Delete|Move)$', path.join(pluginRoot, 'hooks', 'check-agent-files.mjs')]],
  PostToolUse: [['Bash', path.join(pluginRoot, 'hooks', 'audit-bash-result.mjs')]],
  PostToolUseFailure: [['Bash', path.join(pluginRoot, 'hooks', 'audit-bash-result.mjs')]],
};
const entries = {};
for (const [event, expectedEntries] of Object.entries(expectedScripts)) {
  entries[event] = [];
  for (const [matcher, expectedScript] of expectedEntries) {
    const matches = Array.isArray(events[event]) ? events[event].filter((entry) => entry?.matcher === matcher) : [];
    assert.equal(matches.length, 1, `${event} must contain one ${matcher} hook`);
    const entry = matches[0];
    assert.equal(Object.hasOwn(entry, 'description'), false, `${event} description is not supported by ZCode 0.16.5`);
    assert.equal(entry.hooks?.length, 1, `${event} hook shape changed`);
    assert.equal(entry.hooks[0].command, process.execPath, `${event} command changed`);
    assert.equal(entry.hooks[0].args?.length, 1, `${event} args changed`);
    assert.equal(entry.hooks[0].args[0], expectedScript, `${event} wrapper changed`);
    entries[event].push(entry);
  }
}
const root = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-hooks-preflight-'));
fs.writeFileSync(path.join(root, 'README.md'), 'safe\n');
const canary = path.join(root, 'canary');
fs.writeFileSync(canary, 'unchanged\n');
const fileWrapper = entries.PreToolUse.find((entry) => entry.matcher !== 'Bash').hooks[0].args[0];
const safe = spawnSync(process.execPath, [fileWrapper], { input: `${JSON.stringify({ tool_name: 'Read', cwd: root, tool_input: { path: 'README.md' } })}\n`, encoding: 'utf8', env: { ...process.env, ZCODE_AGENT_POLICY: '1', ZCODE_AGENT_WORKSPACE_ROOT: root, ZCODE_AGENT_WRITE_MANIFEST: '[]' } });
assert.match(safe.stdout, /"permissionDecision":"allow"/u);
assert.equal(fs.readFileSync(canary, 'utf8'), 'unchanged\n');
const generation = JSON.parse(fs.readFileSync(provenancePath, 'utf8'));
const hashFile = (file) => crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
assert.equal(generation.effective_config_path, path.resolve(configPath));
assert.equal(generation.effective_config_sha256, hashFile(configPath));
assert.equal(generation.effective_file_policy_version, 'zcode-agent-file-policy/v1.0.0');
assert.equal(generation.effective_file_policy_path, path.join(pluginRoot, 'lib', 'agent-file-policy.mjs'));
assert.equal(generation.effective_file_policy_sha256, hashFile(generation.effective_file_policy_path));
assert.equal(generation.effective_audit_wrapper_path, entries.PostToolUse[0].hooks[0].args[0]);
assert.equal(generation.effective_audit_wrapper_sha256, hashFile(entries.PostToolUse[0].hooks[0].args[0]));
assert.equal(generation.effective_file_wrapper_path, entries.PreToolUse.find((entry) => entry.matcher !== 'Bash').hooks[0].args[0]);
assert.equal(generation.effective_file_wrapper_sha256, hashFile(generation.effective_file_wrapper_path));
generation.hook_activation_verified = true;
const temporary = `${provenancePath}.tmp-${process.pid}`;
fs.writeFileSync(temporary, `${JSON.stringify(generation, null, 2)}\n`, { mode: 0o600 });
fs.renameSync(temporary, provenancePath);
console.log(JSON.stringify({ ok: true, code: 'AGENT_FILE_POLICY_VERIFIED', activation_generation: generation.activation_generation }));
