import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { installPlan, runInit } from '../../cli/installer.mjs';
import { productPaths } from '../../cli/paths.mjs';
import { ZCODE_RUNTIME } from '../../cli/constants.mjs';

test('dry-run reports the entire plan and creates nothing', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-as-subagent-init-'));
  const paths = productPaths(home);
  const result = runInit({ paths, dryRun: true });
  assert.deepEqual(result.plan.map((step) => step.id), [
    'probe-runtime', 'create-data', 'configure-models', 'write-product-config', 'install-launch-agent',
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
  fs.writeFileSync(paths.state, JSON.stringify({ schema_version: 1, completed: ['probe-runtime', 'create-data', 'configure-models'] }));
  const result = runInit({ paths, resume: true, skipRuntimeProbe: true, skipNativeProbe: true });
  assert.equal(result.resumed, true);
  assert.equal(result.completed.filter((id) => id === 'configure-models').length, 1);
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
});

test('plan uses no PATH lookup and points only at fixed bundle runtime', () => {
  const rendered = JSON.stringify(installPlan(productPaths('/tmp/isolated-home')));
  assert.match(rendered, /\/Applications\/ZCode\.app\/Contents\/Resources\/glm\/zcode\.cjs/);
  assert.doesNotMatch(rendered, /which|\/usr\/bin\/env|ZCODE_RUNTIME_PATH/);
});

for (const failStep of ['write-product-config', 'install-launch-agent']) {
  test(`init rolls back every artifact on injected ${failStep} failure`, () => {
    const home = fs.mkdtempSync(path.join(os.tmpdir(), `zcode-as-subagent-rollback-${failStep}-`));
    const paths = productPaths(home);
    fs.mkdirSync(paths.data, { recursive: true });
    fs.writeFileSync(path.join(paths.data, 'keep.txt'), 'keep-existing-data');
    const originalCatalog = Buffer.from('{"provider":{"zai":{"models":{}}},"model":{"main":"old"}}\n');
    const originalProvenance = Buffer.from('prior-provenance-bytes\n');
    const originalState = Buffer.from('{"schema_version":1,"completed":["probe-runtime"]}\n');
    const originalConfig = Buffer.from('prior-config-bytes\n');
    const originalLaunchAgent = Buffer.from('prior-launch-agent-bytes\n');
    fs.mkdirSync(path.dirname(paths.zcodeConfig), { recursive: true });
    fs.writeFileSync(paths.zcodeConfig, originalCatalog);
    fs.writeFileSync(paths.provenance, originalProvenance);
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
      [paths.zcodeConfig, originalCatalog],
      [paths.provenance, originalProvenance],
      [paths.state, originalState],
      [paths.config, originalConfig],
      [paths.launchAgent, originalLaunchAgent],
    ]) assert.deepEqual(fs.readFileSync(file), expected, file);
    assert.equal(fs.readFileSync(path.join(paths.data, 'keep.txt'), 'utf8'), 'keep-existing-data');
    assert.equal(fs.existsSync(paths.logs), false, 'new logs directory must be removed');
  });
}
