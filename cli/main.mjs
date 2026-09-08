import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { BUSINESS_COMMANDS, PRODUCT_NAME, VERSION, ZCODE_RUNTIME } from './constants.mjs';
import { CliError } from './errors.mjs';
import { runInit, installHooks, installMcp, installPlan, nativeBinary } from './installer.mjs';
import { backupData, cleanupLegacy, purge, restoreData, uninstall } from './maintenance.mjs';
import { platform, productPaths } from './paths.mjs';
import { startService, stopService } from './service.mjs';
import { callDaemon, parseDaemonInput } from './rpc.mjs';

const HELP = `zas ${VERSION}\n\nUsage: zas <command> [options]\n\nCommands:\n  help, version               Show basic product information\n  init [--dry-run] [--resume] [--install-hooks] Install and configure the local service\n  hooks install [--dry-run]  Install ZCode policy hooks explicitly\n  install-mcp [codex] [--dry-run|--uninstall] Install or remove the Codex MCP configuration\n  status, diagnose            Inspect local service and runtime state\n  backup --output <dir>       Back up retained product data\n  restore --input <dir>       Verify and restore product data\n  uninstall                   Remove service registration; retain data\n  purge --yes                 Explicitly delete new product data\n  cleanup-legacy --yes        Delete old unpublished installation (no migration)\n`;
const DAEMON_HELP = `  create/spawn, get/poll, list, send, respond, cancel, result, close, observe\n                             Daemon calls accept --json '<object>' or JSON stdin\n                             list JSON requires repository (workspace is an alias)\n                             observe JSON requires only agent_id\n`;

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

function fileArtifact(target, source, capturedAtMs = Date.now()) {
  const artifact = { path: target, source, captured_at_ms: capturedAtMs };
  try {
    const stat = fs.lstatSync(target);
    if (!stat.isFile() || stat.isSymbolicLink()) return artifact;
    artifact.sha256 = crypto.createHash('sha256').update(fs.readFileSync(target)).digest('hex');
  } catch {}
  return artifact;
}

function redactDiagnosticText(text) {
  return text
    .replace(/(Authorization\s*:\s*(?:Bearer\s+)?)(["']?)[A-Za-z0-9._~+/=-]+\2/giu, '$1$2[REDACTED]$2')
    .replace(/(Bearer\s+)(["']?)[A-Za-z0-9._~+/=-]+\2/giu, '$1$2[REDACTED]$2')
    .replace(/((?:["']?)(?:token|secret|password|api[_-]?key|authorization|private[_-]?key)(?:["']?)\s*[=:]\s*)(["']?)([^\s,;"']+)\2/giu, '$1$2[REDACTED]$2')
    .replace(/-----BEGIN [^-]*PRIVATE KEY-----[\s\S]*?-----END [^-]*PRIVATE KEY-----/gu, '[REDACTED_PRIVATE_KEY]');
}

function redactFailureFields(record) {
  // Decode known fields before redacting embedded JSON credential values.
  return JSON.stringify(Object.fromEntries(
    ['agent_id', 'session_id', 'stage', 'error_code', 'message', 'stderr_tail', 'operation', 'remote_code', 'remote_message', 'cleanup_result']
      .filter((field) => typeof record[field] === 'string' || record[field] === null || (field === 'remote_code' && Number.isSafeInteger(record[field])))
      .map((field) => [field, typeof record[field] === 'string' ? redactDiagnosticText(record[field]) : record[field]]),
  ));
}

// Budget the serialized fields, so escaping and UTF-8 cannot invalidate JSON.
// Metadata has bounded prefixes; stderr receives the remaining budget as a tail.
function boundedFailureRecord(record) {
  const redacted = redactFailureFields(record);
  if (Buffer.byteLength(redacted) <= DIAGNOSTIC_TAIL_BYTES) return { text: redacted, truncated: false };
  const fields = JSON.parse(redacted);
  for (const key of Object.keys(fields)) {
    if (key !== 'stderr_tail' && typeof fields[key] === 'string') {
      fields[key] = Array.from(fields[key]).slice(0, key === 'message' ? 512 : 256).join('');
    }
  }
  const tail = Array.from(fields.stderr_tail || '');
  let low = 0;
  let high = tail.length;
  while (low < high) {
    const keep = Math.ceil((low + high) / 2);
    fields.stderr_tail = tail.slice(tail.length - keep).join('');
    if (Buffer.byteLength(JSON.stringify(fields)) <= DIAGNOSTIC_TAIL_BYTES) low = keep;
    else high = keep - 1;
  }
  fields.stderr_tail = tail.slice(tail.length - low).join('');
  return { text: JSON.stringify(fields), truncated: true };
}

function redactDiagnosticTail(text) {
  let incomplete = false;
  const decoded = text.split('\n').map((line) => {
    const match = line.match(/^(\[zcode-agentd\] failure agent=[^:\r\n]+: )(.+)$/u);
    if (match && match[2].startsWith('{')) {
      try {
        const record = JSON.parse(match[2]);
        if (record && typeof record.agent_id === 'string') return redactDiagnosticText(match[1]) + redactFailureFields(record);
      } catch {
        incomplete = true;
        return match[1] + '[INCOMPLETE_FAILURE_RECORD]';
      }
    }
    return line;
  }).join('\n');
  return { text: redactDiagnosticText(decoded), incomplete };
}

function diagnosticLogs(logDirectory) {
  const incomplete = [];
  if (!fs.existsSync(logDirectory)) return { directory: logDirectory, complete: false, incomplete: ['log_directory_missing'], files: [] };
  const files = [];
  let totalBytes = 0;
  for (const name of DIAGNOSTIC_LOG_NAMES) {
    const target = path.join(logDirectory, name);
    let targetStat;
    try { targetStat = fs.lstatSync(target); } catch (error) { targetStat = null; }
    const rotated = ['.1', '.2', '.old', '.gz'].some((suffix) => {
      try { return fs.lstatSync(`${target}${suffix}`) != null; } catch { return false; }
    });
    if (rotated) incomplete.push(`log_rotated:${name}`);
    if (!fs.existsSync(target)) continue;
    if (!targetStat || !targetStat.isFile() || targetStat.isSymbolicLink()) {
      incomplete.push(`log_unreadable:${name}:symlink_or_non_file`);
      continue;
    }
    try {
      const stat = fs.statSync(target);
      const remaining = Math.max(0, DIAGNOSTIC_TOTAL_BYTES - totalBytes);
      const take = Math.min(DIAGNOSTIC_TAIL_BYTES, remaining);
      const start = Math.max(0, stat.size - take);
      // A complete producer record can exceed the display window. Keep a
      // bounded record-sized lookbehind so its prefix survives until decoding.
      const readStart = Math.max(0, start - DIAGNOSTIC_RECORD_BYTES);
      const readLength = Math.min(take + (start - readStart), stat.size - readStart);
      const fd = fs.openSync(target, 'r');
      const buffer = Buffer.alloc(readLength);
      const read = fs.readSync(fd, buffer, 0, readLength, readStart);
      fs.closeSync(fd);
      let decodeStart = Math.max(0, start - 256) - readStart;
      if (decodeStart > 0) {
        const lineStart = buffer.lastIndexOf(0x0a, decodeStart - 1) + 1;
        if (buffer.subarray(lineStart, read).toString('utf8').startsWith('[zcode-agentd] failure agent=')) decodeStart = lineStart;
      }
      // Legacy text keeps its original lookbehind; known records are decoded
      // (or marked incomplete) before any display clipping can hide the prefix.
      const redacted = redactDiagnosticTail(buffer.subarray(decodeStart, read).toString('utf8'));
      if (redacted.incomplete) incomplete.push(`record_incomplete:${name}`);
      const encoded = Buffer.from(redacted.text, 'utf8');
      let tailStart = Math.max(0, encoded.length - Math.min(take, remaining));
      while (tailStart < encoded.length && (encoded[tailStart] & 0xc0) === 0x80) tailStart += 1;
      const bounded = encoded.subarray(tailStart);
      const tail = bounded.toString('utf8');
      totalBytes += Buffer.byteLength(tail);
      if (start > 0 || take < stat.size || bounded.length < encoded.length) incomplete.push(`log_truncated:${name}`);
      files.push({ name, read_status: 'read', bytes: stat.size, modified_at_ms: stat.mtimeMs, rotated, truncated: start > 0 || take < stat.size || bounded.length < encoded.length, tail });
    } catch (error) { incomplete.push(`log_unreadable:${name}:${error.code || 'error'}`); }
  }
  if (files.length === 0) incomplete.push('log_files_missing');
  return { directory: logDirectory, complete: incomplete.length === 0, incomplete, total_bytes: totalBytes, files };
}

// The writer retains the current file and two 1 MiB rotations. Search that
// finite window, independently of the much smaller global display tails.
const DIAGNOSTIC_RETAINED_BYTES = 1024 * 1024;
const DIAGNOSTIC_RECORD_BYTES = 192 * 1024;
function agentDiagnosticLogs(logDirectory, agentId) {
  const report = { status: 'target_record_missing', scope: 'retained_logs', scan_complete: true, scanned_bytes: 0, record: null, incomplete: [] };
  for (const name of ['daemon-error.log', 'daemon-error.log.1', 'daemon-error.log.2']) {
    let fd;
    try {
      const target = path.join(logDirectory, name);
      const stat = fs.lstatSync(target);
      if (!stat.isFile() || stat.isSymbolicLink()) throw Object.assign(new Error('not a regular file'), { code: 'NON_FILE' });
      fd = fs.openSync(target, 'r');
      const size = fs.fstatSync(fd).size;
      const start = Math.max(0, size - DIAGNOSTIC_RETAINED_BYTES);
      const bytes = Buffer.alloc(Math.min(size, DIAGNOSTIC_RETAINED_BYTES));
      const read = fs.readSync(fd, bytes, 0, bytes.length, start);
      report.scanned_bytes += read;
      if (start > 0 || read < bytes.length) report.incomplete.push(`scan_truncated:${name}`);
      const text = bytes.subarray(0, read).toString('utf8');
      const lines = text.split('\n');
      if (start > 0) lines.shift(); // Never associate a partial first record.
      if (lines.pop()) report.incomplete.push(`record_incomplete:${name}`); // The writer may still be appending.
      for (const line of lines.reverse()) {
        if (Buffer.byteLength(line) > DIAGNOSTIC_RECORD_BYTES) { report.incomplete.push(`record_truncated:${name}`); continue; }
        const prefix = `[zcode-agentd] failure agent=${agentId}: `;
        if (!line.startsWith(prefix)) continue;
        const raw = line.slice(prefix.length);
        // JSON records repeat the identifier; reject misleading prefix matches.
        let structured;
        try { structured = JSON.parse(raw); } catch { structured = null; }
        if (structured && structured.agent_id !== agentId) continue;
        if (structured) {
          report.record = { file: name, ...boundedFailureRecord(structured) };
        } else {
          const encoded = Buffer.from(redactDiagnosticText(raw));
          let start = Math.max(0, encoded.length - DIAGNOSTIC_TAIL_BYTES);
          while (start < encoded.length && (encoded[start] & 0xc0) === 0x80) start += 1;
          report.record = { file: name, text: encoded.subarray(start).toString('utf8'), truncated: start > 0 };
        }
        report.status = 'found';
        break;
      }
    } catch (error) {
      if (error.code !== 'ENOENT') report.incomplete.push(`scan_unreadable:${name}:${error.code || 'error'}`);
    } finally { if (fd !== undefined) fs.closeSync(fd); }
    if (report.record) break;
  }
  report.scan_complete = report.incomplete.length === 0;
  return report;
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
  const socket = process.env.ZCODE_AGENTD_SOCKET || paths.socket;
  const report = {
    schema_version: 1,
    scope: agent ? { agent_id: agent } : { kind: 'global' },
    platform: platform(),
    runtime: {
      configured_artifact: fileArtifact(ZCODE_RUNTIME, 'cli_packaged_configuration'),
      running_identity: null,
      running_identity_source: 'not_observed_by_cli',
    },
    facade: {
      running_identity: null,
      running_identity_source: 'not_observed_by_cli',
      packaged_artifact: fileArtifact(nativeBinary('zcode-as-subagent-mcp'), 'distributed_payload'),
    },
    daemon: { socket, socket_exists: fs.existsSync(socket), query_status: 'unqueried', available: null },
    logs: diagnosticLogs(paths.logs),
  };
  try {
    report.daemon.status = await callDaemon(socket, 'status', {});
    report.daemon.available = true;
    report.daemon.query_status = 'queried';
  } catch (error) {
    report.daemon.available = Boolean(error.daemonResponded);
    report.daemon.query_status = error.daemonResponded ? 'query_failed' : 'unavailable';
    report.daemon.error = { code: error.code || 'DAEMON_ERROR', message: error.message };
  }
  if (agent) {
    const diagnostics = agentDiagnosticLogs(paths.logs, agent);
    try {
      const snapshot = await callDaemon(socket, 'poll', { agent_id: agent, timeout_ms: 0 });
      report.daemon.available = true;
      report.agent = {
        diagnostics,
        task: snapshot.task,
        activity: snapshot.activity,
        session_id: snapshot.task?.session_id ?? null,
        turn_id: snapshot.task?.turn_id ?? null,
        request_ids: Array.isArray(snapshot.pending_requests) ? snapshot.pending_requests.map((request) => request.request_id).filter(Boolean) : [],
        query_request_id: snapshot.__request_id ?? null,
        identifiers_complete: Boolean(snapshot.task?.turn_id || (Array.isArray(snapshot.pending_requests) && snapshot.pending_requests.some((request) => request.request_id))),
        pending_request_count: Array.isArray(snapshot.pending_requests) ? snapshot.pending_requests.length : null,
        result_available: snapshot.result_available ?? false,
        observed_at_ms: Date.now(),
      };
    } catch (error) {
      report.daemon.error = { code: error.code || 'DAEMON_ERROR', message: error.message };
      const missing = error.code === 'not_found';
      report.logs.incomplete.push(missing ? 'agent_missing' : 'agent_store_unavailable');
      report.logs.complete = false;
      report.agent = { agent_id: agent, query_status: missing ? 'missing' : 'unavailable', unavailable: !missing, missing, diagnostics };
    }
  }
  if (outputDirectory) {
    const destination = path.resolve(outputDirectory);
    const target = path.join(destination, 'diagnose.json');
    report.output = { path: target, complete: report.logs.complete && report.daemon.query_status === 'queried' && !report.agent?.unavailable && !report.agent?.missing };
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
    const local = { installed: fs.existsSync(paths.state), launch_agent: fs.existsSync(paths.launchAgent), data: fs.existsSync(paths.data) };
    try {
      output({ ...local, daemon_status: await callDaemon(process.env.ZCODE_AGENTD_SOCKET || paths.socket, 'status', {}) });
    } catch (error) {
      output({ ...local, daemon_status: null, daemon_error: { code: error.code || 'DAEMON_ERROR', message: error.message } });
    }
    return;
  }
  if (command === 'diagnose') {
    const report = await diagnose(paths, args.slice(1));
    output({
      ...report,
      daemon_packaged_artifact: fileArtifact(nativeBinary('zcode-as-subagentd'), 'distributed_payload'),
    }); return;
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

export { DAEMON_HELP, HELP, installPlan, diagnose, diagnosticLogs, fileArtifact };
