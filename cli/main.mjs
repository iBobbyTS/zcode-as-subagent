import fs from 'node:fs';
import path from 'node:path';
import { BUSINESS_COMMANDS, PRODUCT_NAME, VERSION, ZCODE_RUNTIME } from './constants.mjs';
import { CliError } from './errors.mjs';
import { runInit, installHooks, installMcp, installPlan, nativeBinary } from './installer.mjs';
import { backupData, cleanupLegacy, purge, restoreData, uninstall } from './maintenance.mjs';
import { platform, productPaths } from './paths.mjs';
import { startService, stopService } from './service.mjs';
import { callDaemon, parseDaemonInput } from './rpc.mjs';

const HELP = `zcode-as-subagent ${VERSION}\n\nUsage: zcode-as-subagent <command> [options]\n\nCommands:\n  help, version               Show basic product information\n  init [--dry-run] [--resume] [--install-hooks] Install and configure the local service\n  hooks install [--dry-run]  Install ZCode policy hooks explicitly\n  install-mcp [codex] [--dry-run|--uninstall] Install or remove the Codex MCP configuration\n  status, diagnose            Inspect local service and runtime state\n  backup --output <dir>       Back up retained product data\n  restore --input <dir>       Verify and restore product data\n  uninstall                   Remove service registration; retain data\n  purge --yes                 Explicitly delete new product data\n  cleanup-legacy --yes        Delete old unpublished installation (no migration)\n`;
const DAEMON_HELP = `  create/spawn, get/poll, list, send, respond, cancel, result, close\n                             Daemon calls accept --json '<object>' or JSON stdin\n                             list JSON requires repository (workspace is an alias)\n`;

function value(args, name) {
  const index = args.indexOf(name);
  if (index < 0) return undefined;
  const result = args[index + 1];
  if (!result || result.startsWith('--')) throw new CliError('INVALID_ARGUMENT', `${name} requires a value`, 2);
  return result;
}

function output(valueToWrite) {
  process.stdout.write(`${JSON.stringify({ ok: true, product: PRODUCT_NAME, ...valueToWrite }, null, 2)}\n`);
}

const DIAGNOSTIC_TAIL_BYTES = 16 * 1024;
const DIAGNOSTIC_TOTAL_BYTES = 32 * 1024;
const DIAGNOSTIC_LOG_NAMES = ['daemon.log', 'daemon-error.log'];

function redactDiagnosticText(text) {
  return text
    .replace(/((?:token|secret|password|api[_-]?key|authorization|private[_-]?key)\s*[=:]\s*)([^\s,;"']+)/giu, '$1[REDACTED]')
    .replace(/(Bearer\s+)[A-Za-z0-9._~+/=-]+/giu, '$1[REDACTED]')
    .replace(/-----BEGIN [^-]*PRIVATE KEY-----[\s\S]*?-----END [^-]*PRIVATE KEY-----/gu, '[REDACTED_PRIVATE_KEY]');
}

function diagnosticLogs(logDirectory) {
  const incomplete = [];
  if (!fs.existsSync(logDirectory)) return { directory: logDirectory, complete: false, incomplete: ['log_directory_missing'], files: [] };
  const files = [];
  let totalBytes = 0;
  for (const name of DIAGNOSTIC_LOG_NAMES) {
    const target = path.join(logDirectory, name);
    const rotated = ['.1', '.old', '.gz'].some((suffix) => fs.existsSync(`${target}${suffix}`));
    if (rotated) incomplete.push(`log_rotated:${name}`);
    if (!fs.existsSync(target)) continue;
    try {
      const stat = fs.statSync(target);
      const bytes = fs.readFileSync(target);
      const remaining = Math.max(0, DIAGNOSTIC_TOTAL_BYTES - totalBytes);
      const take = Math.min(DIAGNOSTIC_TAIL_BYTES, remaining);
      const start = Math.max(0, bytes.length - take);
      const tail = redactDiagnosticText(bytes.subarray(start).toString('utf8'));
      totalBytes += Buffer.byteLength(tail);
      files.push({ name, bytes: stat.size, modified_at_ms: stat.mtimeMs, rotated, truncated: start > 0 || take < bytes.length, tail });
      if (start > 0) incomplete.push(`log_tail_truncated:${name}`);
    } catch (error) { incomplete.push(`log_unreadable:${name}:${error.code || 'error'}`); }
  }
  if (files.length === 0) incomplete.push('log_files_missing');
  return { directory: logDirectory, complete: incomplete.length === 0, incomplete, total_bytes: totalBytes, files };
}

function diagnoseInput(args) {
  const agent = value(args, '--agent');
  const outputDirectory = value(args, '--output');
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (!arg.startsWith('--')) throw new CliError('INVALID_ARGUMENT', `unexpected diagnose argument: ${arg}`, 2);
    if (!['--agent', '--output'].includes(arg)) throw new CliError('INVALID_ARGUMENT', `unsupported diagnose option: ${arg}`, 2);
    index += 1;
  }
  return { agent, outputDirectory };
}

async function diagnose(paths, args) {
  const { agent, outputDirectory } = diagnoseInput(args);
  const report = {
    schema_version: 1,
    scope: agent ? { agent_id: agent } : { kind: 'global' },
    platform: platform(),
    runtime: { path: ZCODE_RUNTIME, exists: fs.existsSync(ZCODE_RUNTIME) },
    daemon: { socket: paths.socket, socket_exists: fs.existsSync(paths.socket), available: false },
    logs: diagnosticLogs(paths.logs),
  };
  if (agent) {
    try {
      const snapshot = await callDaemon(paths.socket, 'poll', { agent_id: agent, timeout_ms: 0 });
      report.daemon.available = true;
      report.agent = {
        task: snapshot.task,
        activity: snapshot.activity,
        session_id: snapshot.session_id ?? snapshot.zcode_session_id ?? snapshot.task?.session_id ?? snapshot.task?.zcode_session_id ?? null,
        turn_id: snapshot.turn_id ?? snapshot.task?.turn_id ?? null,
        request_ids: Array.isArray(snapshot.pending_requests) ? snapshot.pending_requests.map((request) => request.request_id).filter(Boolean) : [],
        pending_request_count: Array.isArray(snapshot.pending_requests) ? snapshot.pending_requests.length : null,
        result_available: snapshot.result_available ?? false,
        observed_at_ms: Date.now(),
      };
    } catch (error) {
      report.daemon.error = { code: error.code || 'DAEMON_ERROR', message: error.message };
      report.logs.incomplete.push('agent_store_unavailable');
      report.logs.complete = false;
      report.agent = { agent_id: agent, unavailable: true };
    }
  }
  if (outputDirectory) {
    const destination = path.resolve(outputDirectory);
    const target = path.join(destination, 'diagnose.json');
    report.output = { path: target, complete: report.logs.complete && !report.agent?.unavailable };
    try {
      fs.mkdirSync(destination, { recursive: true, mode: 0o700 });
      fs.writeFileSync(target, `${JSON.stringify(report, null, 2)}\n`, { mode: 0o600 });
    } catch (error) {
      report.output = { path: target, complete: false, error: { code: error.code || 'OUTPUT_WRITE_FAILED', message: error.message } };
    }
  }
  return report;
}

export async function main(args) {
  const command = args[0] || 'help';
  if (command === 'help' || command === '--help' || command === '-h') {
    process.stdout.write(HELP + DAEMON_HELP); return;
  }
  if (command === 'version' || command === '--version' || command === '-v') {
    process.stdout.write(`${VERSION}\n`); return;
  }
  if (!BUSINESS_COMMANDS.has(command)) throw new CliError('UNKNOWN_COMMAND', `unknown command: ${command}`, 2);
  if (platform() !== 'darwin') throw new CliError('UNSUPPORTED_PLATFORM', `${command} is supported only on macOS`);

  const paths = productPaths();
  if (command === 'init') {
    output(runInit({ paths, dryRun: args.includes('--dry-run'), resume: args.includes('--resume'), installHooks: args.includes('--install-hooks') })); return;
  }
  if (command === 'hooks') {
    if (args[1] !== 'install') throw new CliError('INVALID_ARGUMENT', 'usage: hooks install [--dry-run]', 2);
    output(installHooks(paths, { dryRun: args.includes('--dry-run') })); return;
  }
  if (command === 'install-mcp') {
    const target = args.slice(1).find((arg) => !arg.startsWith('--')) || 'codex';
    if (target !== 'codex') throw new CliError('INVALID_ARGUMENT', `unsupported MCP target: ${target}`, 2);
    output(installMcp(paths, { dryRun: args.includes('--dry-run'), uninstall: args.includes('--uninstall') })); return;
  }
  if (command === 'status') {
    output({ installed: fs.existsSync(paths.state), launch_agent: fs.existsSync(paths.launchAgent), data: fs.existsSync(paths.data) }); return;
  }
  if (command === 'diagnose') {
    const report = await diagnose(paths, args.slice(1));
    output({ ...report, daemon_binary: nativeBinary('zcode-as-subagentd'), daemon_binary_exists: fs.existsSync(nativeBinary('zcode-as-subagentd')) }); return;
  }
  if (command === 'backup') { output(backupData(value(args, '--output'), paths)); return; }
  if (command === 'restore') { output(restoreData(value(args, '--input'), paths)); return; }
  if (command === 'start') { output(startService(paths)); return; }
  if (command === 'stop') { output(stopService(paths)); return; }
  if (command === 'uninstall') { output(uninstall(paths)); return; }
  if (command === 'purge') {
    if (!args.includes('--yes')) throw new CliError('CONFIRMATION_REQUIRED', 'purge requires --yes');
    output(purge(paths)); return;
  }
  if (command === 'cleanup-legacy') {
    if (!args.includes('--yes')) throw new CliError('CONFIRMATION_REQUIRED', 'cleanup-legacy requires --yes');
    output(cleanupLegacy(paths.home)); return;
  }
  const input = parseDaemonInput(args.slice(1));
  const result = await callDaemon(process.env.ZCODE_AGENTD_SOCKET || paths.socket, command, input);
  output({ command, result });
}

export { DAEMON_HELP, HELP, installPlan, diagnose, diagnosticLogs };
