import assert from 'node:assert/strict';
import fs from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { diagnose, diagnosticLogs } from '../../cli/main.mjs';

function pathsFor(home) {
  return {
    home,
    socket: path.join(home, 'agent.sock'),
    logs: path.join(home, 'logs'),
  };
}

test('global diagnose is bounded and marks missing logs incomplete', async () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-diagnose-global-'));
  const report = await diagnose(pathsFor(home), []);
  assert.equal(report.scope.kind, 'global');
  assert.equal(report.logs.complete, false);
  assert.ok(report.logs.incomplete.includes('log_directory_missing'));
  assert.equal(report.agent, undefined);
});

test('agent diagnose reads only the public poll projection and exports a bounded report', async () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-diagnose-agent-'));
  const paths = pathsFor(home);
  fs.mkdirSync(paths.logs, { recursive: true });
  fs.writeFileSync(path.join(paths.logs, 'daemon.log'), 'observed fact\n');
  fs.writeFileSync(path.join(paths.logs, 'daemon.log.1'), 'rotated fact\n');
  fs.writeFileSync(path.join(paths.logs, 'daemon-error.log.2'), 'rotated error\n');
  fs.writeFileSync(path.join(paths.logs, 'secret-extra.log'), 'password=do-not-read\n');
  const server = net.createServer((socket) => socket.once('data', (chunk) => {
    const request = JSON.parse(chunk);
    assert.equal(request.method, 'task_poll');
    socket.end(JSON.stringify({ version: 12, request_id: request.request_id, outcome: 'success', result: {
      kind: 'task_poll', task: { agent_id: 'agent-1', phase: 'RUNNING', outcome: null, reason_code: null, stop_requested: false, close_requested: false, closed: false, reaped: false },
      revision: 3, next_revision: 3, pending_requests: [], result_available: false,
      activity: { state: 'active', active_tools: [], window_60s: {}, telemetry_status: 'healthy' }, latest_progress: null,
      result: null, instruction: 'Use poll for progress', timed_out: false,
    } }) + '\n');
  }));
  await new Promise((resolve) => server.listen(paths.socket, resolve));
  const destination = path.join(home, 'export');
  try {
    const report = await diagnose(paths, ['--agent', 'agent-1', '--output', destination]);
    assert.equal(report.daemon.available, true);
    assert.equal(report.agent.task.agent_id, 'agent-1');
    assert.deepEqual(report.agent.request_ids, []);
    assert.match(report.agent.query_request_id, /^cli-/u);
    assert.equal(report.agent.identifiers_complete, false);
    assert.equal(report.logs.complete, false);
    assert.ok(report.logs.incomplete.some((reason) => reason.startsWith('log_rotated:')));
    assert.ok(report.logs.incomplete.includes('log_rotated:daemon-error.log'));
    assert.equal(report.output.path, path.join(destination, 'diagnose.json'));
    const exported = JSON.parse(fs.readFileSync(report.output.path, 'utf8'));
    assert.equal(exported.agent.task.agent_id, 'agent-1');
    assert.equal(exported.agent.result, undefined);
    assert.equal(exported.agent.task.prompt, undefined);
  } finally { await new Promise((resolve) => server.close(resolve)); }
});

test('diagnostic tail is bounded and reports truncation', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-diagnose-tail-'));
  const logs = path.join(home, 'logs');
  fs.mkdirSync(logs);
  fs.writeFileSync(path.join(logs, 'daemon.log'), 'x'.repeat(20 * 1024));
  const report = diagnosticLogs(logs);
  assert.equal(report.complete, false);
  assert.equal(report.files[0].truncated, true);
  assert.ok(Buffer.byteLength(report.files[0].tail) <= 16 * 1024);
});

test('diagnostic reads only known files, caps total bytes, and redacts secrets', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-diagnose-redact-'));
  const logs = path.join(home, 'logs');
  fs.mkdirSync(logs);
  fs.writeFileSync(path.join(logs, 'daemon.log'), 'token=abc123 password=hunter2 Authorization: Bearer xyz\n' + 'a'.repeat(20 * 1024));
  fs.writeFileSync(path.join(logs, 'daemon-error.log'), 'api_key=secret\n' + 'b'.repeat(20 * 1024));
  fs.writeFileSync(path.join(logs, 'unrelated.log'), 'password=must-not-read');
  fs.writeFileSync(path.join(home, 'outside.log'), 'outside=must-not-read');
  fs.unlinkSync(path.join(logs, 'daemon-error.log'));
  fs.symlinkSync(path.join(home, 'outside.log'), path.join(logs, 'daemon-error.log'));
  const report = diagnosticLogs(logs);
  assert.equal(report.files.length, 1);
  assert.equal(report.total_bytes <= 32 * 1024, true);
  const joined = report.files.map((file) => file.tail).join('\n');
  assert.doesNotMatch(joined, /abc123|hunter2|xyz|secret/u);
  assert.equal(joined.includes('must-not-read'), false);
});

test('diagnostic redacts quoted bearer and JSON secret values', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-diagnose-json-secret-'));
  const logs = path.join(home, 'logs');
  fs.mkdirSync(logs);
  fs.writeFileSync(path.join(logs, 'daemon.log'), '{"password":"hunter2","Authorization":"Bearer xyz","api_key":"abc"}\n');
  const report = diagnosticLogs(logs);
  assert.doesNotMatch(report.files[0].tail, /hunter2|xyz|abc/u);
  assert.match(report.files[0].tail, /REDACTED/u);
});

test('diagnostic export write failure is reported without throwing', async () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-diagnose-output-'));
  const paths = pathsFor(home);
  const report = await diagnose(paths, ['--output', '/dev/null/zcode-diagnose-output']);
  assert.equal(report.output.complete, false);
  assert.ok(report.output.error);
});
