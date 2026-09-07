import fs from 'node:fs';
import crypto from 'node:crypto';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { LAUNCH_AGENT_LABEL, ZCODE_RUNTIME } from './constants.mjs';
import { CliError } from './errors.mjs';
import { atomicWrite, jsonBytes, readOptional, restoreOptional, sha256 } from './fs-atomic.mjs';
import { codexConfigPath, productPaths } from './paths.mjs';

const packageRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const hookInstaller = path.join(packageRoot, 'plugins', 'zcode-subagent-mcp', 'scripts', 'install-agent-hooks.mjs');

export function nativeBinary(name) {
  return path.join(packageRoot, 'npm', 'native', 'darwin-arm64', name);
}

const CODEX_MCP_SECTION = 'mcp_servers.zcode_as_subagent';

function codexMcpConfig(paths) {
  const command = nativeBinary('zcode-as-subagent-mcp');
  return `[${CODEX_MCP_SECTION}]\ncommand = ${JSON.stringify(command)}\nenabled = true\nrequired = true\nstartup_timeout_sec = 10\ntool_timeout_sec = 10\nenabled_tools = [\n  "zcode_subagent_cancel",\n  "zcode_subagent_close",\n  "zcode_subagent_list",\n  "zcode_subagent_poll",\n  "zcode_subagent_respond",\n  "zcode_subagent_result",\n  "zcode_subagent_send",\n  "zcode_subagent_spawn",\n  "zcode_subagent_status",\n]\ndefault_tools_approval_mode = "prompt"\n\n[${CODEX_MCP_SECTION}.env]\nZCODE_AGENTD_SOCKET = ${JSON.stringify(paths.socket)}\n\n[${CODEX_MCP_SECTION}.tools.zcode_subagent_status]\napproval_mode = "auto"\n\n[${CODEX_MCP_SECTION}.tools.zcode_subagent_list]\napproval_mode = "auto"\n\n[${CODEX_MCP_SECTION}.tools.zcode_subagent_poll]\napproval_mode = "auto"\n\n[${CODEX_MCP_SECTION}.tools.zcode_subagent_result]\napproval_mode = "auto"\n`;
}

function removeTomlSection(text, section) {
  const lines = text.split(/(?<=\n)/u);
  let removing = false;
  const kept = [];
  for (const line of lines) {
    const match = line.match(/^\s*\[([^\]]+)\]\s*\r?\n?$/u);
    if (match) removing = match[1] === section || match[1].startsWith(`${section}.`);
    if (!removing) kept.push(line);
  }
  return kept.join('').replace(/\n{3,}$/u, '\n\n');
}

export function installMcp(paths = productPaths(), options = {}) {
  const config = options.configPath || codexConfigPath(paths.home);
  const command = nativeBinary('zcode-as-subagent-mcp');
  if (options.dryRun) {
    return { dry_run: true, operation: options.uninstall ? 'uninstall' : 'install', platform: 'codex', config, command, socket: paths.socket };
  }
  if (options.uninstall) {
    const prior = readOptional(config);
    if (prior === null) return { uninstalled: false, platform: 'codex', config };
    const preserved = removeTomlSection(prior.toString('utf8'), CODEX_MCP_SECTION).replace(/^\n+|\n+$/gu, '');
    atomicWrite(config, Buffer.from(preserved ? `${preserved}\n` : ''));
    return { uninstalled: true, platform: 'codex', config };
  }
  if (!fs.existsSync(command) && !options.skipNativeProbe) {
    throw new CliError('NATIVE_BINARY_NOT_FOUND', 'npm package does not contain the macOS MCP binary');
  }
  const prior = readOptional(config);
  const base = prior === null ? '' : prior.toString('utf8');
  const preserved = removeTomlSection(base, CODEX_MCP_SECTION).replace(/\s*$/u, '');
  const next = `${preserved ? `${preserved}\n\n` : ''}${codexMcpConfig(paths)}`;
  atomicWrite(config, Buffer.from(next));
  return { installed: true, platform: 'codex', config, command, socket: paths.socket };
}

export function installPlan(paths = productPaths(), options = {}) {
  const plan = [
    { id: 'probe-runtime', action: 'verify fixed ZCode runtime', path: ZCODE_RUNTIME },
    { id: 'create-data', action: 'create private product data and log directories', paths: [paths.data, paths.logs] },
    { id: 'write-product-config', action: 'write product paths and fixed runtime', path: paths.config },
    { id: 'install-launch-agent', action: 'install daemon LaunchAgent', path: paths.launchAgent, label: LAUNCH_AGENT_LABEL },
  ];
  if (options.installHooks) plan.push({ id: 'install-hooks', action: 'install ZCode policy hooks', path: paths.zcodeConfig, provenance: paths.hookProvenance });
  return plan;
}

export function installHooks(paths = productPaths(), options = {}) {
  if (options.dryRun) return { dry_run: true, plan: [{ id: 'install-hooks', action: 'install ZCode policy hooks', path: paths.zcodeConfig, provenance: paths.hookProvenance }] };
  const result = spawnSync(process.execPath, [hookInstaller, '--config', paths.zcodeConfig, '--provenance', paths.hookProvenance], { encoding: 'utf8' });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new CliError('HOOK_INSTALL_FAILED', (result.stderr || 'hook installation failed').trim());
  try { return JSON.parse(result.stdout); } catch { throw new CliError('HOOK_INSTALL_FAILED', 'hook installer returned invalid JSON'); }
}

function plist(paths) {
  const daemon = nativeBinary('zcode-as-subagentd');
  const esc = (value) => value.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
  return Buffer.from(`<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n<plist version="1.0"><dict>\n<key>Label</key><string>${LAUNCH_AGENT_LABEL}</string>\n<key>ProgramArguments</key><array><string>${esc(daemon)}</string><string>--database</string><string>${esc(paths.database)}</string><string>--socket</string><string>${esc(paths.socket)}</string><string>--runtime</string><string>${esc(ZCODE_RUNTIME)}</string><string>--diagnostic-log</string><string>${esc(path.join(paths.logs, 'daemon-error.log'))}</string></array>\n<key>EnvironmentVariables</key><dict><key>PATH</key><string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string></dict>\n<key>RunAtLoad</key><true/><key>KeepAlive</key><true/>\n<key>StandardOutPath</key><string>${esc(path.join(paths.logs, 'daemon.log'))}</string>\n<key>StandardErrorPath</key><string>${esc(path.join(paths.logs, 'daemon-error.log'))}</string>\n</dict></plist>\n`);
}

function loadState(file) {
  const bytes = readOptional(file);
  if (bytes === null) return { schema_version: 1, completed: [] };
  try { return JSON.parse(bytes); } catch { throw new CliError('INVALID_INSTALL_STATE', 'install state is invalid JSON'); }
}

function snapshotFile(file) {
  const bytes = readOptional(file);
  return { bytes, sha256: bytes === null ? null : sha256(bytes) };
}

function restoreSnapshotFile(file, snapshot) {
  if (snapshot.bytes !== null && sha256(snapshot.bytes) !== snapshot.sha256) {
    throw new Error(`snapshot hash mismatch for ${file}`);
  }
  restoreOptional(file, snapshot.bytes);
  const restored = readOptional(file);
  if ((snapshot.bytes === null && restored !== null)
    || (snapshot.bytes !== null && (!restored || sha256(restored) !== snapshot.sha256))) {
    throw new Error(`rollback verification failed for ${file}`);
  }
}

export function runInit(options = {}) {
  const paths = options.paths || productPaths();
  const plan = installPlan(paths, options);
  if (options.dryRun) return { dry_run: true, plan };
  if (!fs.existsSync(ZCODE_RUNTIME) && !options.skipRuntimeProbe) {
    throw new CliError('ZCODE_RUNTIME_NOT_FOUND', `required ZCode runtime is missing: ${ZCODE_RUNTIME}`);
  }
  if (!fs.existsSync(nativeBinary('zcode-as-subagentd')) && !options.skipNativeProbe) {
    throw new CliError('NATIVE_BINARY_NOT_FOUND', 'npm package does not contain the macOS daemon binary');
  }
  const prior = {
    files: {
      zcodeConfig: snapshotFile(paths.zcodeConfig),
      hookProvenance: snapshotFile(paths.hookProvenance),
      state: snapshotFile(paths.state),
      config: snapshotFile(paths.config),
      launchAgent: snapshotFile(paths.launchAgent),
    },
    directories: {
      data: fs.existsSync(paths.data),
      logs: fs.existsSync(paths.logs),
    },
  };
  const state = options.resume ? loadState(paths.state) : { schema_version: 1, completed: [] };
  const completed = new Set(state.completed || []);
  const mark = (id) => {
    completed.add(id);
    atomicWrite(paths.state, jsonBytes({ schema_version: 1, completed: [...completed] }));
  };
  const failAt = (id) => {
    if (options._failStep === id) throw new Error(`injected failure at ${id}`);
  };
  try {
    if (!completed.has('probe-runtime')) mark('probe-runtime');
    if (!completed.has('create-data')) {
      fs.mkdirSync(paths.data, { recursive: true, mode: 0o700 });
      fs.mkdirSync(paths.logs, { recursive: true, mode: 0o700 });
      mark('create-data');
    }
    if (!completed.has('write-product-config')) {
      atomicWrite(paths.config, jsonBytes({ schema_version: 1, runtime: ZCODE_RUNTIME, database: paths.database, socket: paths.socket }));
      failAt('write-product-config');
      mark('write-product-config');
    }
    if (!completed.has('install-launch-agent')) {
      atomicWrite(paths.launchAgent, plist(paths), 0o600);
      failAt('install-launch-agent');
      mark('install-launch-agent');
    }
    if (options.installHooks && !completed.has('install-hooks')) {
      installHooks(paths);
      mark('install-hooks');
    }
  } catch (error) {
    const rollbackErrors = [];
    for (const [name, file] of Object.entries({
      zcodeConfig: paths.zcodeConfig,
      hookProvenance: paths.hookProvenance,
      state: paths.state,
      config: paths.config,
      launchAgent: paths.launchAgent,
    })) {
      try { restoreSnapshotFile(file, prior.files[name]); } catch (rollbackError) { rollbackErrors.push(rollbackError); }
    }
    for (const [name, directory] of Object.entries({ data: paths.data, logs: paths.logs })) {
      if (!prior.directories[name] && fs.existsSync(directory)) {
        try { fs.rmSync(directory, { recursive: true, force: true }); } catch (rollbackError) { rollbackErrors.push(rollbackError); }
      }
    }
    if (rollbackErrors.length > 0) error.rollbackErrors = rollbackErrors;
    throw error;
  }
  return { installed: true, resumed: Boolean(options.resume), completed: [...completed], runtime: ZCODE_RUNTIME };
}
