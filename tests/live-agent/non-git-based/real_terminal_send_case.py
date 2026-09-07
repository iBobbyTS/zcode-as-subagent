#!/usr/bin/env python3
"""Opt-in real MCP terminal-send evidence; retain all execution artifacts.

Exit 1 means a transport/protocol/harness error, not a business verdict.
The executing agent evaluates the initial and follow-up tool/results evidence.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import plistlib
import queue
import sqlite3
import subprocess
import threading
import time
import uuid

from fixture_workspace import create_execution_root, materialize

ROOT = Path(__file__).resolve().parents[3]
SOURCE = Path(__file__).resolve().parent / 'fixtures' / 'terminal-send'
MCP_BINARY = ROOT / 'npm/native/darwin-arm64/zcode-as-subagent-mcp'


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--timeout-sec', type=float, default=120)
    parser.add_argument('--observe-sec', type=float, default=20)
    parser.add_argument('--plist', type=Path, default=Path.home() / 'Library/LaunchAgents/com.zcode-as-subagent.daemon.plist')
    args = parser.parse_args()
    if args.timeout_sec <= 0 or args.observe_sec <= 0:
        parser.error('time bounds must be positive')
    execution = create_execution_root('real-terminal-send-')
    repository = materialize(SOURCE, execution)
    print(execution, flush=True)
    errors = []
    agent_id = None
    client = None

    def emit(kind, value):
        with (execution / 'transcript.jsonl').open('a') as stream:
            stream.write(json.dumps({'utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'kind': kind, 'value': value}) + '\n')

    def error(stage, exc):
        errors.append({'stage': stage, 'error': str(exc)})
        emit('error', errors[-1])

    class MCP:
        def __init__(self, socket):
            self.ident = 0
            self.lines = queue.Queue()
            self.stderr = (execution / 'mcp-stderr.log').open('a')
            self.process = subprocess.Popen([str(MCP_BINARY)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr, text=True, env={**os.environ, 'ZCODE_AGENTD_SOCKET': socket})
            def read():
                for line in self.process.stdout:
                    self.lines.put(line)
                self.lines.put(None)
            threading.Thread(target=read, daemon=True).start()

        def rpc(self, method, params):
            self.ident += 1
            payload = {'jsonrpc': '2.0', 'id': self.ident, 'method': method, 'params': params}
            emit('request', {'pid': self.process.pid, **payload})
            self.process.stdin.write(json.dumps(payload) + '\n')
            self.process.stdin.flush()
            deadline = time.monotonic() + 20
            while True:
                line = self.lines.get(timeout=max(.001, deadline - time.monotonic()))
                if line is None:
                    raise RuntimeError('MCP stdout closed')
                response = json.loads(line)
                emit('response', {'pid': self.process.pid, **response})
                if response.get('id') == self.ident:
                    if 'error' in response:
                        raise RuntimeError(response['error'])
                    return response['result']
                if time.monotonic() >= deadline:
                    raise TimeoutError('MCP harness response deadline')

        def initialize(self):
            self.rpc('initialize', {'protocolVersion': '2024-11-05', 'capabilities': {}, 'clientInfo': {'name': 'real-terminal-send-case', 'version': '1'}})
            self.process.stdin.write(json.dumps({'jsonrpc': '2.0', 'method': 'notifications/initialized'}) + '\n')
            self.process.stdin.flush()

        def call(self, method, payload):
            result = self.rpc('tools/call', {'name': 'zcode_subagent_' + method, 'arguments': payload})
            if result.get('isError'):
                raise RuntimeError(result.get('content'))
            return result.get('structuredContent') or json.loads(result['content'][0]['text'])

        def stop(self):
            self.process.stdin.close()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=5)
            self.process.stdout.close()
            self.stderr.close()
            emit('facade_exit', {'pid': self.process.pid, 'exit': self.process.returncode})

    try:
        plist = plistlib.loads(args.plist.read_bytes())
        program = plist['ProgramArguments']
        socket = program[program.index('--socket') + 1]
        database = program[program.index('--database') + 1]
        runtime = program[program.index('--runtime') + 1]
        paths = [Path(program[0]), MCP_BINARY, Path(runtime), ROOT / 'target/release/zcode-as-subagentd', ROOT / 'target/release/zcode-as-subagent-mcp']
        metadata = {'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(), 'program': program, 'hashes': {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}}
        service = subprocess.run(['launchctl', 'print', f'gui/{os.getuid()}/{plist["Label"]}'], capture_output=True, text=True, timeout=10)
        (execution / 'launchctl.txt').write_text(service.stdout + service.stderr)
        emit('environment', metadata)
        if service.returncode:
            raise RuntimeError('LaunchAgent is unavailable')
        client = MCP(socket)
        client.initialize()
        spawned = client.call('spawn', {'repository': str(repository), 'permission_mode': 'plan', 'prompt': 'Use the Read tool to read initial.txt only and report its exact contents. Do not read other files.'})
        agent_id = spawned.get('agent_id') or spawned.get('task', {}).get('agent_id')
        if not agent_id:
            raise RuntimeError('spawn returned no agent_id')
        revision = spawned.get('revision', 0)
        deadline = time.monotonic() + args.timeout_sec
        while time.monotonic() < deadline:
            snapshot = client.call('poll', {'agent_id': agent_id, 'after_revision': revision, 'timeout_ms': 5000})
            revision = snapshot.get('next_revision', revision)
            if snapshot.get('pending_requests'):
                emit('pending_requests', snapshot['pending_requests'])
            if snapshot.get('task', {}).get('phase') == 'TERMINAL':
                break
        else:
            raise TimeoutError('initial task wall-clock limit')
        initial = client.call('result', {'agent_id': agent_id})
        emit('initial_result', initial)
        task = snapshot['task']
        if task.get('outcome') != 'COMPLETED' or task.get('closed') or task.get('close_requested'):
            raise RuntimeError('terminal-send precondition not met')
        client.stop()
        client = MCP(socket)
        client.initialize()
        message = {'agent_id': agent_id, 'message_id': 'terminal-send-' + str(uuid.uuid4()), 'content': 'Use the Read tool to read followup.txt and report its exact contents.'}
        for label in ('send', 'same_id_retry'):
            try:
                emit(label, client.call('send', message))
            except Exception as exc:
                error(label, exc)
        # Observe the whole window: old COMPLETED is not proof of a new turn.
        deadline = time.monotonic() + args.observe_sec
        while time.monotonic() < deadline:
            snapshot = client.call('poll', {'agent_id': agent_id, 'after_revision': revision, 'timeout_ms': 1000})
            revision = snapshot.get('next_revision', revision)
            time.sleep(.25)
        emit('followup_result', client.call('result', {'agent_id': agent_id}))
        with sqlite3.connect(Path(database).as_uri() + '?mode=ro', uri=True) as connection:
            emit('message_rows', connection.execute('SELECT message_id,state,failure_code FROM messages WHERE agent_id=?', (agent_id,)).fetchall())
        diagnosis = subprocess.run(['node', str(ROOT / 'bin/zcode-as-subagent.mjs'), 'diagnose', '--agent', agent_id], capture_output=True, text=True, timeout=20, env={**os.environ, 'ZCODE_AGENTD_SOCKET': socket})
        (execution / 'diagnose.txt').write_text(diagnosis.stdout + diagnosis.stderr)
        emit('diagnose_exit', diagnosis.returncode)
    except Exception as exc:
        error('flow', exc)
    finally:
        if agent_id and client:
            try:
                emit('close', client.call('close', {'agent_id': agent_id}))
                emit('after_close', client.call('poll', {'agent_id': agent_id, 'timeout_ms': 0}))
            except Exception as exc:
                error('cleanup', exc)
        if client:
            client.stop()
        (execution / 'summary.json').write_text(json.dumps({'agent_id': agent_id, 'errors': errors, 'business_verdict': 'requires_evaluation'}, indent=2))
    return int(bool(errors))


if __name__ == '__main__':
    raise SystemExit(main())
