#!/usr/bin/env python3
"""Bounded restart oracle for a durable failed task (not active runtime resume)."""
import argparse
import hashlib
import json
import os
import socket
import sqlite3
import subprocess
import tempfile
import time
from pathlib import Path


class Session:
    def __init__(self, path):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(5)
        self.sock.connect(str(path.with_suffix('.mcp')))
        self.reader = self.sock.makefile('rb')
        self.ident = 0

    def call(self, method, params):
        self.ident += 1
        self.sock.sendall((json.dumps({'jsonrpc': '2.0', 'id': self.ident,
                                      'method': method, 'params': params}) + '\n').encode())
        while True:
            line = self.reader.readline(1024 * 1024)
            if not line:
                raise EOFError('MCP connection closed')
            reply = json.loads(line)
            if reply.get('id') == self.ident:
                if 'error' in reply:
                    raise RuntimeError(reply['error'])
                return reply['result']

    def initialize(self):
        result = self.call('initialize', {'protocolVersion': '2024-11-05', 'capabilities': {},
                                        'clientInfo': {'name': 's01-lifecycle', 'version': '1'}})
        self.sock.sendall(b'{"jsonrpc":"2.0","method":"notifications/initialized"}\n')
        return result

    def tool(self, name, arguments):
        result = self.call('tools/call', {'name': 'zcode_subagent_' + name, 'arguments': arguments})
        if result.get('isError'):
            raise RuntimeError(result)
        if 'structuredContent' in result:
            return result['structuredContent']
        return json.loads(result['content'][0]['text'])

    def close(self):
        self.reader.close()
        self.sock.close()


def wait_for(proc, path):
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f'daemon exited: {proc.returncode}')
        if path.exists() and path.with_suffix('.mcp').exists():
            return
        time.sleep(.05)
    raise TimeoutError('daemon sockets did not appear')


def stop(proc):
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
    return proc.returncode


def state(db):
    with sqlite3.connect(f'file:{db}?mode=ro', uri=True) as conn:
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        counts = {name: conn.execute('SELECT COUNT(*) FROM "' + name.replace('"', '""') + '"').fetchone()[0]
                  for name in tables}
        return counts


def run(daemon, root):
    db, sock = root / 'state.db', root / 'd.sock'
    facts = {'binary': str(daemon), 'binary_sha256': hashlib.sha256(daemon.read_bytes()).hexdigest(),
             'database': str(db), 'socket': str(sock), 'mcp_socket': str(sock.with_suffix('.mcp')),
             'started_at_epoch': time.time(), 'task_recovery': False,
             'fixture_runtime': '/usr/bin/false', 'active_runtime_resume_tested': False,
             'recovery_scope': 'existing terminal task and result queried through MCP after restart'}
    procs, sessions, logs = [], [], []
    env = dict(os.environ)
    env.pop('ZCODE_AGENTD_TEST_STARTUP_GATE', None)
    def start(index):
        out, err = root / f'daemon-{index}.stdout', root / f'daemon-{index}.stderr'
        handles = [out.open('w'), err.open('w')]
        logs.extend(handles)
        proc = subprocess.Popen([str(daemon), '--database', str(db), '--socket', str(sock),
                                 '--runtime', '/usr/bin/false'],
                                env=env, stdout=handles[0], stderr=handles[1])
        procs.append(proc)
        facts[f'daemon_{index}'] = {'pid': proc.pid, 'stdout': str(out), 'stderr': str(err)}
        wait_for(proc, sock)
        session = Session(sock)
        sessions.append(session)
        session.initialize()
        facts[f'{index}_initialize'] = True
        tools = session.call('tools/list', {})
        facts[f'{index}_tools_list'] = True
        facts[f'{index}_tool_count'] = len(tools['tools'])
        return proc, session, tools
    try:
        first, old, first_tools = start('first')
        repository = root / 'repository'
        repository.mkdir()
        facts['spawn'] = old.tool('spawn', {'repository': str(repository), 'permission_mode': 'plan',
                                           'prompt': 'S01 isolated terminal recovery fixture'})
        agent_id = facts['spawn']['agent_id']
        facts['agent_id'] = agent_id
        deadline = time.monotonic() + 15
        while True:
            poll = old.tool('poll', {'agent_id': agent_id, 'timeout_ms': 0})
            if poll['task']['phase'] == 'TERMINAL':
                break
            if time.monotonic() > deadline:
                raise TimeoutError('fixture task did not become terminal')
            time.sleep(.1)
        facts['poll_before'] = poll
        facts['result_before'] = old.tool('result', {'agent_id': agent_id})
        if facts['result_before']['result'] is None:
            raise RuntimeError('terminal fixture has no durable result')
        before = state(db)
        # Also exercise the original two-connection EOF-isolation scenario.
        smoke = subprocess.run([os.sys.executable, str(Path(__file__).with_name('mcp_shared_service_case.py')),
                                '--socket', str(sock)], capture_output=True, text=True, timeout=15)
        facts['direct_live'] = {'exit_code': smoke.returncode, 'stdout': smoke.stdout, 'stderr': smoke.stderr}
        if smoke.returncode:
            raise RuntimeError('direct-live failed')
        facts['first_stop_exit_code'] = stop(first)
        facts['sockets_removed_after_stop'] = not sock.exists() and not sock.with_suffix('.mcp').exists()
        try:
            old.call('tools/list', {})
            facts['old_connection_failed'] = False
        except (EOFError, BrokenPipeError, ConnectionResetError):
            facts['old_connection_failed'] = True
        second, fresh, second_tools = start('second')
        facts['poll_after'] = fresh.tool('poll', {'agent_id': agent_id, 'timeout_ms': 0})
        facts['result_after'] = fresh.tool('result', {'agent_id': agent_id})
        facts['list_after'] = fresh.tool('list', {'repository': str(repository)})
        facts['task_recovery'] = (
            facts['poll_after']['task']['agent_id'] == agent_id
            and facts['poll_after']['task']['phase'] == 'TERMINAL'
            and facts['result_after']['result'] == facts['result_before']['result']
            and any(task['agent_id'] == agent_id for task in facts['list_after']['tasks']))
        after = state(db)
        facts.update({'state_before': before, 'state_after': after,
                      'catalog_state_recovery': first_tools == second_tools and before == after})
        facts['second_stop_exit_code'] = stop(second)
        facts['sockets_removed_after_restart_stop'] = not sock.exists() and not sock.with_suffix('.mcp').exists()
        facts['passed'] = (facts['task_recovery'] and facts['catalog_state_recovery'] and facts['old_connection_failed']
                           and facts['sockets_removed_after_stop'] and facts['sockets_removed_after_restart_stop']
                           and facts['first_stop_exit_code'] == facts['second_stop_exit_code'] == 0)
    except Exception as exc:
        facts.update({'passed': False, 'error': f'{type(exc).__name__}: {exc}'})
    finally:
        for session in sessions:
            session.close()
        for proc in procs:
            stop(proc)
        for handle in logs:
            handle.close()
    facts['ended_at_epoch'] = time.time()
    (root / 'facts.json').write_text(json.dumps(facts, indent=2) + '\n')
    return facts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--daemon', required=True, type=Path)
    args = parser.parse_args()
    workspace = Path(__file__).resolve().parents[1] / 'workspace'
    workspace.mkdir(exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix='s01-', dir=workspace))
    facts = run(args.daemon.resolve(), root)
    print(json.dumps(facts, sort_keys=True))
    raise SystemExit(0 if facts['passed'] else 1)


if __name__ == '__main__':
    main()
