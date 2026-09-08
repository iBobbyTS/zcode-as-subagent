import assert from 'node:assert/strict';
import fs from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { diagnose, diagnosticLogs, fileArtifact } from '../../cli/main.mjs';

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
  assert.equal(report.facade.running_identity, null);
  assert.equal(report.facade.running_identity_source, 'not_observed_by_cli');
});

test('artifact identity hashes only its labeled file and records source and capture time', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-artifact-identity-'));
  const running = path.join(home, 'running');
  const packaged = path.join(home, 'packaged');
  fs.writeFileSync(running, 'old');
  fs.writeFileSync(packaged, 'new');
  const identity = fileArtifact(running, 'running_executable', 17);
  assert.equal(identity.path, running);
  assert.equal(identity.source, 'running_executable');
  assert.equal(identity.captured_at_ms, 17);
  assert.equal(identity.sha256, 'cba06b5736faf67e54b07b561eae94395e774c517a7d910a54369e1263ccfbd4');
  assert.notEqual(identity.sha256, fileArtifact(packaged, 'distributed_payload', 18).sha256);
});

test('diagnose preserves daemon self identity and does not promote packaged facade to running', async () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-diag-identity-'));
  const paths = pathsFor(home);
  const daemonIdentity = {
    daemon: { component: 'daemon', version: '0.1.0', source_revision: 'old-revision', artifact: { path: '/running/old-daemon', sha256: 'old-hash', source: 'running_executable', captured_at_ms: 10 } },
    runtime: { configured_path: '/configured/runtime', configured_path_source: 'daemon_configuration', observed_version_source: 'unknown' },
    models: { configured: { value: 'configured-model', source: 'session_create_configuration' } },
  };
  await withDaemon(paths, (request) => {
    assert.equal(request.method, 'system_status');
    return { outcome: 'success', result: { status: { protocol_version: 12, identity: daemonIdentity } } };
  }, async () => {
    const report = await diagnose(paths, []);
    assert.deepEqual(report.daemon.status.identity, daemonIdentity);
    assert.equal(report.daemon.status.identity.models.observed_response, undefined);
    assert.equal(report.facade.running_identity, null);
    assert.equal(report.facade.packaged_artifact.source, 'distributed_payload');
    assert.equal(report.runtime.running_identity, null);
  });
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

test('diagnostic reads only known regular files and caps total bytes', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-diagnose-bounds-'));
  try {
    const logs = path.join(home, 'logs');
    fs.mkdirSync(logs);
    fs.writeFileSync(path.join(logs, 'daemon.log'), 'a'.repeat(20 * 1024) + '\nKNOWN_DAEMON_MARKER');
    fs.writeFileSync(path.join(logs, 'daemon-error.log'), 'b'.repeat(20 * 1024) + '\nKNOWN_ERROR_MARKER');
    fs.writeFileSync(path.join(logs, 'unrelated.log'), 'UNRELATED_MARKER');
    fs.writeFileSync(path.join(home, 'outside.log'), 'OUTSIDE_MARKER');
    fs.unlinkSync(path.join(logs, 'daemon-error.log'));
    fs.symlinkSync(path.join(home, 'outside.log'), path.join(logs, 'daemon-error.log'));

    const report = diagnosticLogs(logs);
    assert.equal(report.files.length, 1);
    assert.equal(report.files[0].name, 'daemon.log');
    assert.ok(report.total_bytes <= 32 * 1024);
    assert.ok(Buffer.byteLength(report.files[0].tail) <= 16 * 1024);
    assert.match(report.files[0].tail, /KNOWN_DAEMON_MARKER/u);
    assert.doesNotMatch(JSON.stringify(report), /UNRELATED_MARKER|OUTSIDE_MARKER/u);
    assert.ok(report.incomplete.includes('log_unreadable:daemon-error.log:symlink_or_non_file'));
  } finally { fs.rmSync(home, { recursive: true, force: true }); }
});

test('diagnostic marks an unfinished structured failure without publishing a partial record', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-diagnose-partial-'));
  try {
    const logs = path.join(home, 'logs');
    fs.mkdirSync(logs);
    fs.writeFileSync(path.join(logs, 'daemon.log'),
      'prefix'.repeat(4000) + '\n[zcode-agentd] failure agent=Agent-A: {"agent_id":"Agent-A","message":"unfinished');
    const report = diagnosticLogs(logs);
    assert.equal(report.files[0].truncated, true);
    assert.ok(report.files[0].tail.endsWith('[INCOMPLETE_FAILURE_RECORD]'));
    assert.ok(report.incomplete.includes('record_incomplete:daemon.log'));
    assert.ok(Buffer.byteLength(report.files[0].tail) <= 16 * 1024);
  } finally { fs.rmSync(home, { recursive: true, force: true }); }
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

test('Agent A diagnostics survive Agent B displacing global tails and finite rotation', async () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-diag-target-'));
  try {
    const paths = pathsFor(home);
    fs.mkdirSync(paths.logs);
    const target = '[zcode-agentd] failure agent=Agent-A: ' + JSON.stringify({
      agent_id: 'Agent-A', session_id: null, stage: 'bootstrap', error_code: 'SESSION_START_FAILED',
      message: 'A-owned-failure', stderr_tail: 'A-owned-stderr',
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
        assert.doesNotMatch(report.logs.files.map((file) => file.tail).join(''), /A-owned-failure/u);
        assert.equal(report.agent.task.reason_code, 'RUNTIME_START_FAILED');
        assert.equal(report.agent.diagnostics.status, 'found');
        assert.equal(report.agent.diagnostics.record.file, rotated ? 'daemon-error.log.1' : 'daemon-error.log');
        const decoded = JSON.parse(report.agent.diagnostics.record.text);
        assert.equal(decoded.agent_id, 'Agent-A');
        assert.equal(decoded.error_code, 'SESSION_START_FAILED');
        assert.equal(decoded.message, 'A-owned-failure');
        assert.equal(decoded.stderr_tail, 'A-owned-stderr');
        assert.doesNotMatch(report.agent.diagnostics.record.text, /B-owned-failure/u);
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
  } finally { fs.rmSync(home, { recursive: true, force: true }); }
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
    assert.ok(found.text.includes('hide-this'));
    assert.ok(decoded.stderr_tail.endsWith('FINAL_ERROR_MARKER'));
    assert.ok(fs.readFileSync(report.output.path, 'utf8').includes('FINAL_ERROR_MARKER'));
  } finally { fs.rmSync(home, { recursive: true, force: true }); }
});
