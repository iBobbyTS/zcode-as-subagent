import assert from 'node:assert/strict';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { callDaemon } from '../../cli/rpc.mjs';
import { CliError } from '../../cli/errors.mjs';

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
      socket.end(JSON.stringify({ version: 12, outcome: 'success', result: { kind: 'task_poll', timed_out: true } }) + '\n');
    });
  });
  await new Promise((resolve) => server.listen(socketPath, resolve));
  try { assert.deepEqual(await callDaemon(socketPath, 'poll', { agent_id: 'agent-1' }), { kind: 'task_poll', timed_out: true }); }
  finally { await new Promise((resolve) => server.close(resolve)); }
});

test('CLI preserves daemon error code, message, and active agent id', async () => {
  const socketPath = path.join(os.tmpdir(), `zcode-cli-rpc-error-${process.pid}-${Date.now()}.sock`);
  const server = net.createServer((socket) => { socket.once('data', () => socket.end(JSON.stringify({ version: 12, outcome: 'error', error: { code: 'not_found', message: 'task was not found', active_agent_id: 'agent-2' } }) + '\n')); });
  await new Promise((resolve) => server.listen(socketPath, resolve));
  try { await assert.rejects(() => callDaemon(socketPath, 'result', { agent_id: 'agent-2' }), (error) => error instanceof CliError && error.code === 'not_found' && error.agentId === 'agent-2'); }
  finally { await new Promise((resolve) => server.close(resolve)); }
});

test('CLI reports unavailable daemon socket', async () => {
  await assert.rejects(() => callDaemon(path.join(os.tmpdir(), `missing-zcode-${process.pid}.sock`), 'cancel', { agent_id: 'missing' }), (error) => error.code === 'SOCKET_UNAVAILABLE');
});
