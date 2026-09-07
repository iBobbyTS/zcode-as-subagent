import assert from 'node:assert/strict';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { callDaemon, MAX_RESULT_CHUNK_BYTES, projectDaemonResult } from '../../cli/rpc.mjs';
import { CliError } from '../../cli/errors.mjs';
import { DAEMON_HELP } from '../../cli/main.mjs';

test('CLI help documents JSON list scope instead of nonexistent flags', () => {
  assert.match(DAEMON_HELP, /list JSON requires repository \(workspace is an alias\)/u);
  assert.doesNotMatch(DAEMON_HELP, /--repository|--workspace/u);
});

test('CLI sends daemon RPC and preserves success result', async () => {
  const socketPath = path.join(os.tmpdir(), `zcode-cli-rpc-${process.pid}-${Date.now()}.sock`);
  const server = net.createServer((socket) => {
    let body = '';
    socket.on('data', (chunk) => {
      body += chunk;
      if (!body.includes('\n')) return;
      const request = JSON.parse(body);
      assert.equal(request.version, 12);
      assert.equal(request.method, 'task_poll');
      assert.equal(request.params.agent_id, 'agent-1');
      socket.end(JSON.stringify({
        version: 12,
        request_id: request.request_id,
        outcome: 'success',
        result: {
          kind: 'task_poll',
          task: { agent_id: 'agent-1', phase: 'RUNNING', outcome: null, reason_code: null, stop_requested: false, close_requested: false, closed: false, reaped: false },
          revision: 0,
          next_revision: 0,
          pending_requests: [],
          command_pending_approval: false,
          result_available: false,
          activity: { state: 'active', latest_progress: 'private duplicate', active_tools: [], window_60s: {}, telemetry_status: 'healthy' },
          latest_progress: null,
          result: null,
          instruction: 'Use poll for progress',
          timed_out: true,
        },
      }) + '\n');
    });
  });
  await new Promise((resolve) => server.listen(socketPath, resolve));
  try {
    const result = await callDaemon(socketPath, 'poll', { agent_id: 'agent-1' });
    assert.equal(result.task.cancel_requested, false);
    assert.equal(result.task.resources_reaped, false);
    assert.equal(result.activity.latest_progress, undefined);
    assert.equal(result.result, null);
    assert.equal(result.timed_out, true);
    assert.equal(result.kind, undefined);
  }
  finally { await new Promise((resolve) => server.close(resolve)); }
});

test('CLI preserves daemon error code, message, and active agent id', async () => {
  const socketPath = path.join(os.tmpdir(), `zcode-cli-rpc-error-${process.pid}-${Date.now()}.sock`);
  const server = net.createServer((socket) => { socket.once('data', (chunk) => { const request = JSON.parse(chunk); socket.end(JSON.stringify({ version: 12, request_id: request.request_id, outcome: 'error', error: { code: 'not_found', message: 'task was not found', active_agent_id: 'agent-2' } }) + '\n'); }); });
  await new Promise((resolve) => server.listen(socketPath, resolve));
  try { await assert.rejects(() => callDaemon(socketPath, 'result', { agent_id: 'agent-2' }), (error) => error instanceof CliError && error.code === 'not_found' && error.agentId === 'agent-2'); }
  finally { await new Promise((resolve) => server.close(resolve)); }
});

test('CLI rejects daemon responses with wrong RPC version or request id', async () => {
  for (const response of [
    { version: 11, request_id: 'ignored', outcome: 'success', result: {} },
    { version: 12, request_id: 'other-request', outcome: 'success', result: {} },
  ]) {
    const socketPath = path.join(os.tmpdir(), `zcode-cli-rpc-protocol-${process.pid}-${Date.now()}-${response.version}.sock`);
    const server = net.createServer((socket) => { socket.once('data', () => socket.end(JSON.stringify(response) + '\n')); });
    await new Promise((resolve) => server.listen(socketPath, resolve));
    try { await assert.rejects(() => callDaemon(socketPath, 'result', { agent_id: 'agent-1' }), (error) => error instanceof CliError && error.code === 'PROTOCOL_ERROR'); }
    finally { await new Promise((resolve) => server.close(resolve)); }
  }
});

test('CLI rejects list without repository or workspace scope before connecting', async () => {
  assert.throws(() => callDaemon(path.join(os.tmpdir(), `missing-zcode-list-${process.pid}.sock`), 'list', {}), (error) => error instanceof CliError && error.code === 'INVALID_ARGUMENT');
});

test('CLI maps workspace list scope to daemon repository scope', async () => {
  const socketPath = path.join(os.tmpdir(), `zcode-cli-rpc-list-${process.pid}-${Date.now()}.sock`);
  const server = net.createServer((socket) => { socket.once('data', (chunk) => { const request = JSON.parse(chunk); assert.equal(request.params.repository, '/workspace'); socket.end(JSON.stringify({ version: 12, request_id: request.request_id, outcome: 'success', result: { tasks: [] } }) + '\n'); }); });
  await new Promise((resolve) => server.listen(socketPath, resolve));
  try { await callDaemon(socketPath, 'list', { workspace: '/workspace' }); }
  finally { await new Promise((resolve) => server.close(resolve)); }
});

test('CLI applies documented list and result defaults before connecting', async () => {
  for (const [command, input, expected] of [
    ['list', { repository: '/workspace' }, { phase: null, outcome: null, cursor: null, limit: 100 }],
    ['result', { agent_id: 'agent-1' }, { offset: 0, limit: MAX_RESULT_CHUNK_BYTES }],
  ]) {
    const socketPath = path.join(os.tmpdir(), `zcode-cli-rpc-default-${command}-${process.pid}-${Date.now()}.sock`);
    const server = net.createServer((socket) => { socket.once('data', (chunk) => {
      const request = JSON.parse(chunk);
      for (const [name, value] of Object.entries(expected)) assert.deepEqual(request.params[name], value);
      const result = command === 'list'
        ? { kind: 'task_listed', tasks: [], next_cursor: null }
        : { kind: 'task_result', task: { agent_id: 'agent-1', phase: 'RUNNING', outcome: null, reason_code: null, stop_requested: false, close_requested: false, closed: false, reaped: false }, result: null };
      socket.end(JSON.stringify({ version: 12, request_id: request.request_id, outcome: 'success', result }) + '\n');
    }); });
    await new Promise((resolve) => server.listen(socketPath, resolve));
    try { await callDaemon(socketPath, command, input); }
    finally { await new Promise((resolve) => server.close(resolve)); }
  }
});

test('CLI observe uses the shared read-only daemon snapshot without adding fields', async () => {
  const socketPath = path.join(os.tmpdir(), `zcode-cli-rpc-observe-${process.pid}-${Date.now()}.sock`);
  const observation = {
    schema: 'zas-observation/1.1', agent_id: 'agent-1', service_generation: 'generation',
    snapshot_seq: 7, count_scope: 'agent_lifetime', tools: [],
    reasoning: { text: '', char_count: 0, truncated: false, source: { status: 'VERIFIED_RUNTIME_PUBLIC', runtime_version: '3.11.2', event_type: 'model.streaming', delta_pointer: '/params/payload/delta' } },
    coverage: { tool_history_complete: false, reasoning_complete: false, dropped_events: 0 },
  };
  const server = net.createServer((socket) => { socket.once('data', (chunk) => {
    const request = JSON.parse(chunk);
    assert.equal(request.method, 'task_observe');
    assert.deepEqual(request.params, { agent_id: 'agent-1' });
    socket.end(`${JSON.stringify({ version: 12, request_id: request.request_id, outcome: 'success', result: { kind: 'task_observed', observation } })}\n`);
  }); });
  await new Promise((resolve) => server.listen(socketPath, resolve));
  try {
    assert.deepEqual(await callDaemon(socketPath, 'observe', { agent_id: 'agent-1' }), observation);
  } finally { await new Promise((resolve) => server.close(resolve)); }
});

test('CLI public projection removes private RPC fields and result digest', () => {
  const task = { agent_id: 'agent-1', phase: 'TERMINAL', outcome: 'COMPLETED', reason_code: null, stop_requested: false, close_requested: false, closed: false, reaped: true };
  const projected = projectDaemonResult('result', {
    kind: 'task_result',
    task,
    result: { outcome: 'COMPLETED', final_text: 'ok', partial: false, result_sha256: 'private', offset: 0, total_bytes: 2, next_offset: null, complete: true },
  });
  assert.deepEqual(Object.keys(projected).sort(), ['result', 'task']);
  assert.equal(projected.task.resources_reaped, true);
  assert.equal(projected.task.reaped, undefined);
  assert.equal(projected.result.result_sha256, undefined);
});

test('CLI reports unavailable daemon socket', async () => {
  await assert.rejects(() => callDaemon(path.join(os.tmpdir(), `missing-zcode-${process.pid}.sock`), 'cancel', { agent_id: 'missing' }), (error) => error.code === 'SOCKET_UNAVAILABLE');
});
