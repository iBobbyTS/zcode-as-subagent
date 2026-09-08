import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { installMcp, installPlan, runInit, installPlugin } from '../../cli/installer.mjs';
import { codexConfigPath, productPaths } from '../../cli/paths.mjs';
import { ZCODE_RUNTIME } from '../../cli/constants.mjs';

test('dry-run reports the entire plan and creates nothing', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-as-subagent-init-'));
  const paths = productPaths(home);
  const result = runInit({ paths, dryRun: true });
  assert.deepEqual(result.plan.map((step) => step.id), [
    'probe-runtime', 'create-data', 'write-product-config', 'install-launch-agent',
  ]);
  assert.equal(result.plan[0].path, ZCODE_RUNTIME);
  assert.equal(fs.readdirSync(home).length, 0);
});

test('init excludes hooks by default and includes them only when requested', () => {
  const paths = productPaths('/tmp/isolated-home');
  assert.equal(installPlan(paths).some((step) => step.id === 'install-hooks'), false);
  assert.equal(installPlan(paths, { installHooks: true }).some((step) => step.id === 'install-hooks'), true);
});

test('explicit hook installation is available as an independent dry-run', async () => {
  const { installHooks } = await import('../../cli/installer.mjs');
  const paths = productPaths(fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-as-subagent-hooks-')));
  const result = installHooks(paths, { dryRun: true });
  assert.equal(result.dry_run, true);
  assert.equal(result.plan[0].id, 'install-hooks');
  assert.equal(fs.existsSync(paths.zcodeConfig), false);
});

test('init installs hooks only with the explicit opt-in', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-as-subagent-init-hooks-'));
  const paths = productPaths(home);
  const result = runInit({ paths, installHooks: true, skipRuntimeProbe: true, skipNativeProbe: true });
  assert.ok(result.completed.includes('install-hooks'));
  assert.equal(JSON.parse(fs.readFileSync(paths.zcodeConfig, 'utf8')).hooks.enabled, true);
  assert.equal(fs.existsSync(paths.hookProvenance), true);
});

test('resume skips completed steps', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-as-subagent-resume-'));
  const paths = productPaths(home);
  fs.mkdirSync(paths.data, { recursive: true });
  fs.writeFileSync(paths.state, JSON.stringify({ schema_version: 1, completed: ['probe-runtime', 'create-data'] }));
  const result = runInit({ paths, resume: true, skipRuntimeProbe: true, skipNativeProbe: true });
  assert.equal(result.resumed, true);
  assert.equal(result.completed.includes('create-data'), true);
  assert.equal(fs.existsSync(paths.zcodeConfig), false);
  assert.equal(fs.existsSync(paths.launchAgent), true);
});

test('LaunchAgent provides a stable PATH for the Node runtime', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-as-subagent-launch-agent-'));
  const paths = productPaths(home);
  runInit({ paths, skipRuntimeProbe: true, skipNativeProbe: true });
  const launchAgent = fs.readFileSync(paths.launchAgent, 'utf8');
  assert.match(launchAgent, /<key>EnvironmentVariables<\/key><dict>/);
  assert.match(launchAgent, /<key>PATH<\/key><string>\/opt\/homebrew\/bin:\/usr\/local\/bin:\/usr\/bin:\/bin:\/usr\/sbin:\/sbin<\/string>/);
  assert.match(launchAgent, /<key>ProgramArguments<\/key><array>/);
  assert.match(launchAgent, /zcode-as-subagentd<\/string>/);
  assert.ok(launchAgent.includes(`<string>--diagnostic-log</string><string>${path.join(paths.logs, 'daemon-error.log')}</string>`));
  assert.ok(launchAgent.includes(`<key>StandardErrorPath</key><string>${path.join(paths.logs, 'daemon-error.log')}</string>`));
});

test('plan uses no PATH lookup and points only at fixed bundle runtime', () => {
  const rendered = JSON.stringify(installPlan(productPaths('/tmp/isolated-home')));
  assert.match(rendered, /\/Applications\/ZCode\.app\/Contents\/Resources\/glm\/zcode\.cjs/);
  assert.doesNotMatch(rendered, /which|\/usr\/bin\/env|ZCODE_RUNTIME_PATH/);
});

test('install-mcp writes an idempotent Codex config under CODEX_HOME', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-as-subagent-codex-'));
  const codexHome = path.join(home, 'codex-home');
  process.env.CODEX_HOME = codexHome;
  try {
    const paths = productPaths(home);
    fs.mkdirSync(codexHome, { recursive: true });
    fs.writeFileSync(codexConfigPath(home), '[general]\nfoo = true\n\n[mcp_servers.other]\ncommand = "other"\n');

    const first = installMcp(paths, { skipNativeProbe: true });
    const once = fs.readFileSync(first.config, 'utf8');
    const second = installMcp(paths, { skipNativeProbe: true });
    const twice = fs.readFileSync(second.config, 'utf8');

    assert.equal(twice, once);
    assert.match(twice, /\[general\]/);
    assert.match(twice, /\[mcp_servers\.other\]/);
    assert.equal((twice.match(/\[mcp_servers\.zcode_as_subagent\]/g) || []).length, 1);
    assert.match(twice, /zcode-as-subagent-mcp/);
    assert.match(twice, /"zcode_subagent_status"/);
    assert.doesNotMatch(twice, /zcode_subagent_system_status/);
    assert.match(twice, new RegExp(paths.socket.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&')));
  } finally {
    delete process.env.CODEX_HOME;
  }
});

test('install-mcp falls back to ~/.codex and dry-run creates nothing', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-as-subagent-codex-dry-'));
  const paths = productPaths(home);
  const original = process.env.CODEX_HOME;
  delete process.env.CODEX_HOME;
  try {
    const result = installMcp(paths, { dryRun: true });
    assert.equal(result.dry_run, true);
    assert.equal(result.config, path.join(home, '.codex', 'config.toml'));
    assert.equal(fs.existsSync(result.config), false);
  } finally {
    if (original === undefined) delete process.env.CODEX_HOME;
    else process.env.CODEX_HOME = original;
  }
});

test('install-mcp --uninstall removes only the managed Codex section', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-as-subagent-codex-uninstall-'));
  const paths = productPaths(home);
  const config = path.join(home, 'codex', 'config.toml');
  fs.mkdirSync(path.dirname(config), { recursive: true });
  fs.writeFileSync(config, '[general]\nfoo = true\n\n[mcp_servers.zcode_as_subagent]\ncommand = "managed"\n\n[mcp_servers.zcode_as_subagent.env]\nZCODE_AGENTD_SOCKET = "old"\n\n[mcp_servers.other]\ncommand = "other"\n');

  const result = installMcp(paths, { configPath: config, uninstall: true });
  const content = fs.readFileSync(config, 'utf8');
  assert.equal(result.uninstalled, true);
  assert.match(content, /\[general\]/);
  assert.match(content, /\[mcp_servers\.other\]/);
  assert.doesNotMatch(content, /zcode_as_subagent/);
  assert.equal(installMcp(paths, { configPath: config, uninstall: true }).uninstalled, true);
});

test('install-plugin fails closed on marketplace source conflict and missing source can uninstall', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-as-subagent-plugin-conflict-'));
  const paths = productPaths(home);
  const codexHome = path.join(home, 'codex');
  const cli = path.join(home, 'codex-fake');
  fs.writeFileSync(cli, '#!/bin/sh\nexit 0\n', { mode: 0o700 });
  const marketplace = path.join(home, '.agents', 'plugins', 'marketplace.json');
  fs.mkdirSync(path.dirname(marketplace), { recursive: true });
  fs.writeFileSync(marketplace, JSON.stringify({ name: 'personal', plugins: [{ name: 'zcode-as-subagent', source: { source: 'local', path: './other' } }] }));
  assert.throws(() => installPlugin(paths, { home, codexHome, marketplacePath: marketplace, codexCli: cli }), (error) => error.code === 'PLUGIN_MARKETPLACE_CONFLICT');
  const missing = path.join(home, 'missing-plugin');
  assert.doesNotThrow(() => installPlugin(paths, { source: missing, home, codexHome, marketplacePath: marketplace, codexCli: cli, uninstall: true }));
});

for (const failStep of ['write-product-config', 'install-launch-agent']) {
  test(`init rolls back every artifact on injected ${failStep} failure`, () => {
    const home = fs.mkdtempSync(path.join(os.tmpdir(), `zcode-as-subagent-rollback-${failStep}-`));
    const paths = productPaths(home);
    fs.mkdirSync(paths.data, { recursive: true });
    fs.writeFileSync(path.join(paths.data, 'keep.txt'), 'keep-existing-data');
    const originalState = Buffer.from('{"schema_version":1,"completed":["probe-runtime"]}\n');
    const originalConfig = Buffer.from('prior-config-bytes\n');
    const originalLaunchAgent = Buffer.from('prior-launch-agent-bytes\n');
    fs.writeFileSync(paths.state, originalState);
    fs.writeFileSync(paths.config, originalConfig);
    fs.mkdirSync(path.dirname(paths.launchAgent), { recursive: true });
    fs.writeFileSync(paths.launchAgent, originalLaunchAgent);

    assert.throws(() => runInit({
      paths,
      skipRuntimeProbe: true,
      skipNativeProbe: true,
      _failStep: failStep,
    }), new RegExp(`injected failure at ${failStep}`));

    for (const [file, expected] of [
      [paths.state, originalState],
      [paths.config, originalConfig],
      [paths.launchAgent, originalLaunchAgent],
    ]) assert.deepEqual(fs.readFileSync(file), expected, file);
    assert.equal(fs.readFileSync(path.join(paths.data, 'keep.txt'), 'utf8'), 'keep-existing-data');
    assert.equal(fs.existsSync(paths.logs), false, 'new logs directory must be removed');
  });
}
