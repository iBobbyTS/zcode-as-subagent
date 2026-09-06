import net from 'node:net';
import crypto from 'node:crypto';
import fs from 'node:fs';
import { CliError } from './errors.mjs';

export const RPC_VERSION = 12;

function readJsonInput(args) {
  const inline = args.find((arg) => arg.startsWith('--json='));
  if (inline) return JSON.parse(inline.slice('--json='.length));
  const index = args.indexOf('--json');
  if (index >= 0 && args[index + 1] && !args[index + 1].startsWith('--')) return JSON.parse(args[index + 1]);
  if (index >= 0 || !process.stdin.isTTY) {
    const input = fs.readFileSync(0, 'utf8').trim();
    if (input) return JSON.parse(input);
  }
  return {};
}

function requestId() { return `cli-${process.pid}-${crypto.randomUUID()}`; }

function manifest(input) {
  if (!input.repository || !input.prompt) throw new CliError('INVALID_ARGUMENT', 'create requires repository and prompt', 2);
  return {
    schema: 'zcode-general-task/v1', agent_id: 'daemon-prepared', repository: input.repository,
    permission_mode: input.permission_mode || 'build', prompt: input.prompt,
    write_manifest: input.write_manifest || [], scratch_root: os.tmpdir(),
  };
}

function methodFor(command, input) {
  switch (command) {
    case 'create': case 'spawn': return { method: 'submit_general', params: { input: { manifest: manifest(input), allowed_command_ids: input.allowed_command_ids || [], required_command_ids: input.required_command_ids || [] } } };
    case 'get': case 'poll': return { method: 'task_poll', params: { agent_id: input.agent_id, after_revision: input.after_revision || 0, timeout_ms: input.timeout_ms ?? 0 } };
    case 'list': {
      const repository = input.repository ?? input.workspace;
      if (!repository) throw new CliError('INVALID_ARGUMENT', 'list requires repository or workspace scope', 2);
      return { method: 'task_list', params: { repository, phase: input.phase ?? null, outcome: input.outcome ?? null, cursor: input.cursor ?? null, limit: input.limit ?? 100 } };
    }
    case 'send': return { method: 'task_message', params: { agent_id: input.agent_id, message_id: input.message_id || requestId(), mode: input.mode || 'queue', content: input.content } };
    case 'respond': return { method: 'task_respond', params: { agent_id: input.agent_id, request_id: input.request_id, decision: input.decision, content: input.reason ?? input.content ?? null } };
    case 'cancel': return { method: 'task_cancel', params: { agent_id: input.agent_id } };
    case 'result': return { method: 'task_result', params: { agent_id: input.agent_id } };
    case 'close': return { method: 'task_close', params: { agent_id: input.agent_id } };
    default: throw new CliError('UNKNOWN_COMMAND', `unsupported daemon command: ${command}`, 2);
  }
}

export function callDaemon(socketPath, command, input, timeoutMs = 6000) {
  const { method, params } = methodFor(command, input);
  const request_id = requestId();
  const request = JSON.stringify({ version: RPC_VERSION, request_id, method, params }) + '\n';
  return new Promise((resolve, reject) => {
    const socket = net.createConnection(socketPath); let data = ''; let settled = false;
    const finish = (fn, value) => { if (!settled) { settled = true; socket.destroy(); fn(value); } };
    const timer = setTimeout(() => finish(reject, new CliError('SOCKET_UNAVAILABLE', `daemon socket unavailable: ${socketPath}`)), timeoutMs);
    socket.setEncoding('utf8');
    socket.on('connect', () => socket.end(request));
    socket.on('data', (chunk) => {
      data += chunk; const line = data.split('\n')[0]; if (!line) return; clearTimeout(timer);
      try {
        const response = JSON.parse(line);
        if (response.version !== RPC_VERSION || response.request_id !== request_id) finish(reject, new CliError('PROTOCOL_ERROR', 'daemon returned an RPC response for a different version or request'));
        else if (response.outcome === 'error') { const daemon = response.error || {}; const error = new CliError(daemon.code || 'DAEMON_ERROR', daemon.message || 'daemon request failed'); error.agentId = daemon.active_agent_id; finish(reject, error); }
        else if (response.outcome === 'success') finish(resolve, response.result);
        else finish(reject, new CliError('PROTOCOL_ERROR', 'daemon returned an invalid RPC response'));
      } catch { finish(reject, new CliError('PROTOCOL_ERROR', 'daemon returned invalid JSON')); }
    });
    socket.on('error', () => { clearTimeout(timer); finish(reject, new CliError('SOCKET_UNAVAILABLE', `daemon socket unavailable: ${socketPath}`)); });
    socket.on('close', () => { clearTimeout(timer); if (!settled) finish(reject, new CliError('SOCKET_UNAVAILABLE', 'daemon closed the RPC connection')); });
  });
}

export function parseDaemonInput(args) { try { return readJsonInput(args); } catch (error) { throw new CliError('INVALID_JSON', error.message, 2); } }
