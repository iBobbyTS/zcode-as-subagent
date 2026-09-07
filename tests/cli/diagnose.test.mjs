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
    if (request.method === 'system_status') {
      socket.end(JSON.stringify({ version: 12, request_id: request.request_id, outcome: 'success', result: { status: { protocol_version: 12 } } }) + '\n');
      return;
    }
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

test('diagnostic tail distinguishes successful reading from truncated history', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-diagnose-tail-'));
  const logs = path.join(home, 'logs');
  fs.mkdirSync(logs);
  fs.writeFileSync(path.join(logs, 'daemon.log'), 'x'.repeat(20 * 1024));
  const report = diagnosticLogs(logs);
  assert.equal(report.complete, false);
  assert.equal(report.files[0].read_status, 'read');
  assert.ok(report.incomplete.includes('log_truncated:daemon.log'));
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

test('diagnostic preserves legacy multiline redaction and marks partial failure records', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-diagnose-partial-'));
  const logs = path.join(home, 'logs');
  fs.mkdirSync(logs);
  fs.writeFileSync(path.join(logs, 'daemon.log'), 'prefix'.repeat(4000)
    + '\n-----BEGIN PRIVATE KEY-----\nLEGACY_PRIVATE_VALUE\n-----END PRIVATE KEY-----\n'
    + '[zcode-agentd] failure agent=Agent-A: {"agent_id":"Agent-A","message":"unfinished');
  const report = diagnosticLogs(logs);
  assert.equal(report.files[0].truncated, true);
  assert.match(report.files[0].tail, /\[REDACTED_PRIVATE_KEY\]/);
  assert.doesNotMatch(report.files[0].tail, /LEGACY_PRIVATE_VALUE/);
  assert.ok(report.files[0].tail.endsWith('[INCOMPLETE_FAILURE_RECORD]'));
  assert.ok(report.incomplete.includes('record_incomplete:daemon.log'));
  assert.ok(Buffer.byteLength(report.files[0].tail) <= 16 * 1024);
});

test('diagnostic export write failure is reported without throwing', async () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-diagnose-output-'));
  const paths = pathsFor(home);
  const report = await diagnose(paths, ['--output', '/dev/null/zcode-diagnose-output']);
  assert.equal(report.output.complete, false);
  assert.ok(report.output.error);
});

async function withDaemon(paths, respond, run) {
  const server = net.createServer((socket) => socket.once('data', (chunk) => {
    const request = JSON.parse(chunk);
    socket.end(JSON.stringify({ version: 12, request_id: request.request_id, ...respond(request) }) + '\n');
  }));
  await new Promise((resolve) => server.listen(paths.socket, resolve));
  try { return await run(); }
  finally { await new Promise((resolve) => server.close(resolve)); }
}

function statusOrTask(request, agentId = 'Agent-A') {
  if (request.method === 'system_status') return { outcome: 'success', result: { status: { protocol_version: 12, service_generation: 'configured-daemon' } } };
  assert.equal(request.method, 'task_poll');
  assert.equal(request.params.agent_id, agentId);
  return { outcome: 'success', result: {
    task: { agent_id: agentId, phase: 'TERMINAL', outcome: 'FAILED', reason_code: 'RUNTIME_START_FAILED', reaped: true },
    activity: {}, pending_requests: [], result_available: true,
  } };
}

test('global diagnose queries the configured effective socket without model side effects', async () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-diag-socket-'));
  const paths = pathsFor(home);
  const configured = { socket: path.join(home, 'configured.sock') };
  const previous = process.env.ZCODE_AGENTD_SOCKET;
  process.env.ZCODE_AGENTD_SOCKET = configured.socket;
  try {
    await withDaemon(configured, (request) => {
      assert.equal(request.method, 'system_status');
      assert.equal(Object.hasOwn(request, 'params'), false);
      return statusOrTask(request);
    }, async () => {
      const report = await diagnose(paths, []);
      assert.equal(report.daemon.socket, configured.socket);
      assert.equal(report.daemon.available, true);
      assert.equal(report.daemon.query_status, 'queried');
      assert.equal(report.daemon.status.service_generation, 'configured-daemon');
      assert.equal(fs.existsSync(paths.socket), false);
    });
  } finally {
    if (previous === undefined) delete process.env.ZCODE_AGENTD_SOCKET;
    else process.env.ZCODE_AGENTD_SOCKET = previous;
  }
});

test('recent structured failures are redacted in global and agent diagnostic tails and exports', async () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-diag-recent-secret-'));
  const paths = pathsFor(home);
  fs.mkdirSync(paths.logs);
  const failure = {
    agent_id: 'Agent-A', session_id: null, stage: 'bootstrap', error_code: 'SESSION_START_FAILED',
    message: 'recent-failure ' + JSON.stringify({ api_key: 'RECENT_MESSAGE_SECRET' }),
    stderr_tail: JSON.stringify({ password: 'RECENT_STDERR_SECRET' }),
  };
  fs.writeFileSync(path.join(paths.logs, 'daemon-error.log'),
    '[zcode-agentd] failure agent=Agent-A: ' + JSON.stringify(failure) + '\nlegacy token=LEGACY_SECRET\n');
  await withDaemon(paths, statusOrTask, async () => {
    for (const scope of [[], ['--agent', 'Agent-A']]) {
      const report = await diagnose(paths, [...scope, '--output', path.join(home, scope.length ? 'agent' : 'global')]);
      const tail = report.logs.files[0].tail;
      assert.match(tail, /recent-failure/);
      assert.match(tail, /legacy token=\[REDACTED\]/);
      assert.doesNotMatch(tail, /RECENT_MESSAGE_SECRET|RECENT_STDERR_SECRET|LEGACY_SECRET/);
      assert.doesNotMatch(JSON.stringify(report), /RECENT_MESSAGE_SECRET|RECENT_STDERR_SECRET|LEGACY_SECRET/);
      assert.doesNotMatch(fs.readFileSync(report.output.path, 'utf8'), /RECENT_MESSAGE_SECRET|RECENT_STDERR_SECRET|LEGACY_SECRET/);
      const decoded = JSON.parse(tail.split('\n')[0].slice('[zcode-agentd] failure agent=Agent-A: '.length));
      assert.equal(decoded.session_id, null);
      assert.equal(decoded.error_code, failure.error_code);
      assert.match(decoded.message, /REDACTED/);
      assert.match(decoded.stderr_tail, /REDACTED/);
      assert.ok(Buffer.byteLength(tail) <= 16 * 1024);
      if (scope.length) assert.equal(report.agent.diagnostics.status, 'found');
    }
  });
});

test('complete producer records larger than the display window are redacted before clipping', async () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-diag-large-secret-'));
  const paths = pathsFor(home);
  fs.mkdirSync(paths.logs);
  await withDaemon(paths, statusOrTask, async () => {
    for (const padding of ['x', '\u0001']) {
      const failure = {
        agent_id: 'Agent-A', session_id: null, stage: 'bootstrap', error_code: 'SESSION_START_FAILED',
        message: padding.repeat(3000),
        stderr_tail: padding.repeat(16000) + JSON.stringify({ api_key: 'LONG_RECORD_SECRET' }),
      };
      assert.ok(Buffer.byteLength(failure.message) <= 4096);
      assert.ok(Buffer.byteLength(failure.stderr_tail) <= 16384);
      const line = '[zcode-agentd] failure agent=Agent-A: ' + JSON.stringify(failure) + '\n';
      assert.ok(Buffer.byteLength(line) > 16 * 1024 + 256);
      assert.ok(Buffer.byteLength(line) < 192 * 1024);
      // Force a nonzero bounded read offset while retaining the complete failure.
      fs.writeFileSync(path.join(paths.logs, 'daemon-error.log'), 'old-log\n'.repeat(40000) + line);
      for (const scope of [[], ['--agent', 'Agent-A']]) {
        const report = await diagnose(paths, [...scope, '--output', path.join(home, 'export')]);
        const tail = report.logs.files[0].tail;
        assert.equal(report.logs.files[0].truncated, true);
        assert.ok(Buffer.byteLength(tail) <= 16 * 1024);
        assert.match(tail, /REDACTED/);
        assert.doesNotMatch(JSON.stringify(report), /LONG_RECORD_SECRET/);
        assert.doesNotMatch(fs.readFileSync(report.output.path, 'utf8'), /LONG_RECORD_SECRET/);
        if (scope.length) assert.equal(report.agent.diagnostics.status, 'found');
      }
    }
  });
});

test('unfinished structured failure records never fall back to escaped raw secrets', async () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-diag-partial-secret-'));
  const paths = pathsFor(home);
  fs.mkdirSync(paths.logs);
  const record = { agent_id: 'Agent-A', message: 'x'.repeat(3000), stderr_tail: 'x'.repeat(16000) + JSON.stringify({ api_key: 'PARTIAL_RECORD_SECRET' }) };
  // A possible intermediate append: the embedded credential is written, but
  // the outer JSON closing quote/brace and terminating newline are not yet.
  fs.writeFileSync(path.join(paths.logs, 'daemon-error.log'), '[zcode-agentd] failure agent=Agent-A: ' + JSON.stringify(record).slice(0, -2));
  await withDaemon(paths, statusOrTask, async () => {
    for (const scope of [[], ['--agent', 'Agent-A']]) {
      const report = await diagnose(paths, [...scope, '--output', path.join(home, 'export')]);
      assert.equal(report.logs.complete, false);
      assert.ok(report.logs.incomplete.includes('record_incomplete:daemon-error.log'));
      assert.match(report.logs.files[0].tail, /INCOMPLETE_FAILURE_RECORD/);
      assert.doesNotMatch(JSON.stringify(report), /PARTIAL_RECORD_SECRET/);
      assert.doesNotMatch(fs.readFileSync(report.output.path, 'utf8'), /PARTIAL_RECORD_SECRET/);
      if (scope.length) assert.equal(report.agent.diagnostics.status, 'target_record_missing');
    }
  });
});

test('Agent A diagnostics survive Agent B displacing global tails and finite rotation', async () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-diag-target-'));
  const paths = pathsFor(home);
  fs.mkdirSync(paths.logs);
  const target = '[zcode-agentd] failure agent=Agent-A: ' + JSON.stringify({
    agent_id: 'Agent-A', session_id: null, stage: 'bootstrap', error_code: 'SESSION_START_FAILED',
    message: 'A-owned-failure token=private-token ' + JSON.stringify({ password: 'NESTED_MESSAGE_SECRET' }),
    stderr_tail: JSON.stringify({ token: 'NESTED_STDERR_TOKEN', api_key: 'NESTED_STDERR_KEY' }),
  }) + '\n';
  const noise = '[zcode-agentd] failure agent=Agent-B: B-owned-failure\n'.repeat(1000);
  fs.writeFileSync(path.join(paths.logs, 'daemon-error.log'), target + noise);
  await withDaemon(paths, statusOrTask, async () => {
    for (const rotated of [false, true]) {
      if (rotated) {
        fs.renameSync(path.join(paths.logs, 'daemon-error.log'), path.join(paths.logs, 'daemon-error.log.1'));
        fs.writeFileSync(path.join(paths.logs, 'daemon-error.log'), noise);
      }
      const report = await diagnose(paths, ['--agent', 'Agent-A', '--output', path.join(home, 'export')]);
      assert.doesNotMatch(report.logs.files.map((file) => file.tail).join(''), /A-owned-failure/);
      assert.equal(report.agent.task.reason_code, 'RUNTIME_START_FAILED');
      assert.equal(report.agent.diagnostics.status, 'found');
      assert.match(report.agent.diagnostics.record.text, /A-owned-failure/);
      assert.doesNotMatch(report.agent.diagnostics.record.text, /B-owned-failure|private-token|NESTED_MESSAGE_SECRET|NESTED_STDERR_TOKEN|NESTED_STDERR_KEY/);
      assert.doesNotMatch(fs.readFileSync(report.output.path, 'utf8'), /private-token|NESTED_MESSAGE_SECRET|NESTED_STDERR_TOKEN|NESTED_STDERR_KEY/);
      const decoded = JSON.parse(report.agent.diagnostics.record.text);
      assert.equal(decoded.agent_id, 'Agent-A');
      assert.equal(decoded.session_id, null);
      assert.equal(decoded.error_code, 'SESSION_START_FAILED');
      assert.match(decoded.stderr_tail, /REDACTED/);
      assert.equal(report.agent.diagnostics.record.file, rotated ? 'daemon-error.log.1' : 'daemon-error.log');
      assert.ok(report.agent.diagnostics.scanned_bytes <= 3 * 1024 * 1024);
    }
    fs.writeFileSync(path.join(paths.logs, 'daemon-error.log.1'), noise);
    let report = await diagnose(paths, ['--agent', 'Agent-A']);
    assert.equal(report.agent.diagnostics.status, 'target_record_missing');
    assert.equal(report.agent.diagnostics.scan_complete, true);
    fs.writeFileSync(path.join(paths.logs, 'daemon-error.log'), target + noise.repeat(30));
    report = await diagnose(paths, ['--agent', 'Agent-A']);
    assert.equal(report.agent.diagnostics.status, 'target_record_missing');
    assert.equal(report.agent.diagnostics.scan_complete, false);
    assert.ok(report.agent.diagnostics.incomplete.includes('scan_truncated:daemon-error.log'));
  });
});

test('missing agent and unreachable daemon have distinct diagnostic states', async () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-diag-missing-'));
  const paths = pathsFor(home);
  await withDaemon(paths, (request) => request.method === 'system_status' ? statusOrTask(request) : { outcome: 'error', error: { code: 'not_found', message: 'task not found' } }, async () => {
    const report = await diagnose(paths, ['--agent', 'unknown']);
    assert.equal(report.daemon.available, true);
    assert.equal(report.agent.query_status, 'missing');
    assert.equal(report.agent.unavailable, false);
  });
  const report = await diagnose(paths, []);
  assert.equal(report.daemon.available, false);
  assert.equal(report.daemon.query_status, 'unavailable');
});


test('diagnostic output caps UTF-8 bytes and distinguishes unfinished target records', async () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-diag-utf8-'));
  const paths = pathsFor(home);
  fs.mkdirSync(paths.logs);
  const target = '[zcode-agentd] failure agent=Agent-A: ' + JSON.stringify({ agent_id: 'Agent-A', message: '诊断'.repeat(6000) });
  fs.writeFileSync(path.join(paths.logs, 'daemon-error.log'), target + '\n');
  fs.writeFileSync(path.join(paths.logs, 'daemon.log'), '诊断'.repeat(6000));
  await withDaemon(paths, statusOrTask, async () => {
    let report = await diagnose(paths, ['--agent', 'Agent-A']);
    assert.ok(report.logs.total_bytes <= 32 * 1024);
    assert.ok(report.logs.files.every((file) => Buffer.byteLength(file.tail) <= 16 * 1024));
    assert.equal(report.agent.diagnostics.status, 'found');
    assert.equal(report.agent.diagnostics.record.truncated, true);
    assert.ok(Buffer.byteLength(report.agent.diagnostics.record.text) <= 16 * 1024);
    fs.writeFileSync(path.join(paths.logs, 'daemon-error.log'), target);
    report = await diagnose(paths, ['--agent', 'Agent-A']);
    assert.equal(report.agent.diagnostics.status, 'target_record_missing');
    assert.equal(report.agent.diagnostics.scan_complete, false);
    assert.ok(report.agent.diagnostics.incomplete.includes('record_incomplete:daemon-error.log'));
  });
});

for (const tail of ['x'.repeat(16384 - 18) + 'FINAL_ERROR_MARKER', '界\n"'.repeat(2000) + 'FINAL_ERROR_MARKER']) test(`agent export budgets JSON fields and retains final stderr (${Buffer.byteLength(tail)} bytes)`, async () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-diagnose-last-error-'));
  try {
    const paths = pathsFor(home);
    fs.mkdirSync(paths.logs);
    const record = { agent_id: 'target', session_id: 's'.repeat(4096), stage: 'runtime_terminal', error_code: 'SESSION_SEND_FAILED', message: 'm'.repeat(4096), stderr_tail: tail, operation: 'session/send', remote_code: -32031, remote_message: 'model unavailable token=hide-this', cleanup_result: 'Signaled(15)' };
    // Produce a legal driver tail, then evict this record from the global tail.
    fs.writeFileSync(path.join(paths.logs, 'daemon-error.log'), `[zcode-agentd] failure agent=target: ${JSON.stringify(record)}\n` + 'other agent\n'.repeat(4000));
    const report = await diagnose(paths, ['--agent', 'target', '--output', path.join(home, 'out')]);
    assert.equal(report.agent.diagnostics.status, 'found');
    const found = report.agent.diagnostics.record;
    assert.equal(found.truncated, true);
    assert.ok(Buffer.byteLength(found.text) <= 16 * 1024);
    const decoded = JSON.parse(found.text);
    assert.equal(decoded.agent_id, 'target');
    assert.equal(decoded.remote_code, -32031);
    assert.equal(decoded.operation, 'session/send');
    assert.equal(decoded.cleanup_result, 'Signaled(15)');
    assert.ok(!found.text.includes('hide-this'));
    assert.ok(decoded.stderr_tail.endsWith('FINAL_ERROR_MARKER'));
    assert.ok(fs.readFileSync(report.output.path, 'utf8').includes('FINAL_ERROR_MARKER'));
  } finally { fs.rmSync(home, { recursive: true, force: true }); }
});
