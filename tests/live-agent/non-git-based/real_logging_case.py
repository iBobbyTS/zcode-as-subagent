#!/usr/bin/env python3
"""Collect real structured-error and component-identity facts via MCP stdio.

The runner preserves raw tools/call envelopes, including expected isError
results. Exit zero means the bounded transport flow completed; it does not
decide whether the Agent achieved its business goal.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import plistlib
import queue
import subprocess
import threading
import time

from fixture_workspace import create_execution_root, materialize


ROOT = Path(__file__).resolve().parents[3]
SOURCE = Path(__file__).resolve().parent / "fixtures" / "tiny-agent"
RELEASE_MCP = ROOT / "target/release/zcode-as-subagent-mcp"
PACKAGED_MCP = ROOT / "npm/native/darwin-arm64/zcode-as-subagent-mcp"


class RawMcpClient:
    def __init__(self, binary: Path, socket: str, execution: Path, emit):
        self.ident = 0
        self.lines: queue.Queue[str | None] = queue.Queue()
        self.emit = emit
        self.stderr = (execution / "mcp-stderr.log").open("a")
        self.process = subprocess.Popen(
            [str(binary)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=self.stderr, text=True,
            env={**os.environ, "ZCODE_AGENTD_SOCKET": socket},
        )

        def read_stdout():
            for line in self.process.stdout:
                self.lines.put(line)
            self.lines.put(None)

        threading.Thread(target=read_stdout, daemon=True).start()

    def request(self, method: str, params: dict) -> dict:
        self.ident += 1
        request = {"jsonrpc": "2.0", "id": self.ident, "method": method, "params": params}
        self.emit("request", {"pid": self.process.pid, **request})
        self.process.stdin.write(json.dumps(request) + "\n")
        self.process.stdin.flush()
        deadline = time.monotonic() + 20
        while True:
            line = self.lines.get(timeout=max(0.001, deadline - time.monotonic()))
            if line is None:
                raise RuntimeError("MCP stdout closed")
            response = json.loads(line)
            self.emit("response", {"pid": self.process.pid, **response})
            if response.get("id") == self.ident:
                return response
            if time.monotonic() >= deadline:
                raise TimeoutError("MCP response deadline")

    def result(self, method: str, params: dict) -> dict:
        envelope = self.request(method, params)
        if "error" in envelope:
            raise RuntimeError(envelope["error"])
        return envelope["result"]

    def initialize(self) -> None:
        self.result("initialize", {
            "protocolVersion": "2024-11-05", "capabilities": {},
            "clientInfo": {"name": "real-logging-case", "version": "1"},
        })
        self.process.stdin.write(json.dumps({
            "jsonrpc": "2.0", "method": "notifications/initialized",
        }) + "\n")
        self.process.stdin.flush()

    def tool_envelope(self, name: str, arguments: dict) -> dict:
        return self.request("tools/call", {
            "name": "zcode_subagent_" + name, "arguments": arguments,
        })

    def tool_success(self, name: str, arguments: dict) -> dict:
        envelope = self.tool_envelope(name, arguments)
        if "error" in envelope or envelope.get("result", {}).get("isError"):
            raise RuntimeError(envelope)
        return envelope["result"].get("structuredContent")

    def stop(self) -> None:
        if self.process.stdin and not self.process.stdin.closed:
            self.process.stdin.close()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            self.process.wait(timeout=5)
        self.process.stdout.close()
        self.stderr.close()
        self.emit("facade_exit", {"pid": self.process.pid, "exit": self.process.returncode})


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout-sec", type=float, default=120)
    parser.add_argument("--socket")
    parser.add_argument("--mcp-binary", type=Path)
    parser.add_argument(
        "--plist", type=Path,
        default=Path.home() / "Library/LaunchAgents/com.zcode-as-subagent.daemon.plist",
    )
    args = parser.parse_args()
    execution = create_execution_root("real-logging-")
    repository = materialize(SOURCE, execution)
    transcript = execution / "transcript.jsonl"
    errors: list[dict] = []
    agent_id = None
    client = None
    print(execution, flush=True)

    def emit(kind: str, value) -> None:
        with transcript.open("a") as stream:
            stream.write(json.dumps({
                "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "kind": kind, "value": value,
            }, ensure_ascii=False) + "\n")

    try:
        if args.socket:
            socket = args.socket
            daemon_program = None
        else:
            plist = plistlib.loads(args.plist.read_bytes())
            daemon_program = plist["ProgramArguments"]
            socket = daemon_program[daemon_program.index("--socket") + 1]
        binary = args.mcp_binary or (RELEASE_MCP if RELEASE_MCP.exists() else PACKAGED_MCP)
        paths = [binary]
        if daemon_program:
            paths.append(Path(daemon_program[0]))
        emit("environment", {
            "head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
            "socket": socket,
            "paths": {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                      for path in paths if path.is_file()},
        })
        client = RawMcpClient(binary, socket, execution, emit)
        client.initialize()
        emit("catalog", client.result("tools/list", {}))
        emit("status", client.tool_success("status", {}))

        expected = client.tool_envelope("result", {"agent_id": "logging-missing-agent"})
        emit("expected_business_error", expected)
        result = expected.get("result", {})
        if "error" in expected or not result.get("isError"):
            raise RuntimeError("missing-agent call did not return a tool execution error")
        if result.get("structuredContent", {}).get("error", {}).get("code") != "not_found":
            raise RuntimeError("missing-agent structured error code mismatch")

        spawned = client.tool_success("spawn", {
            "repository": str(repository), "permission_mode": "plan",
            "prompt": "Reply with exactly REAL_LOGGING_CASE and do not call tools.",
        })
        agent_id = spawned["agent_id"]
        emit("spawn", spawned)
        revision = 0
        deadline = time.monotonic() + args.timeout_sec
        while time.monotonic() < deadline:
            poll = client.tool_success("poll", {
                "agent_id": agent_id, "after_revision": revision, "timeout_ms": 5000,
            })
            emit("poll", poll)
            revision = poll.get("next_revision", revision)
            if poll.get("task", {}).get("phase") == "TERMINAL":
                break
        else:
            raise TimeoutError("task did not become terminal")

        # FAILED/CANCELLED is a task fact, not a tool execution failure.
        result_envelope = client.tool_envelope("result", {"agent_id": agent_id})
        emit("terminal_result_envelope", result_envelope)
        if "error" in result_envelope or result_envelope.get("result", {}).get("isError"):
            raise RuntimeError("terminal result query became a tool execution error")
        emit("close", client.tool_success("close", {"agent_id": agent_id}))
        agent_id = None
    except Exception as error:
        errors.append({"type": type(error).__name__, "message": str(error)})
        emit("error", errors[-1])
    finally:
        if client:
            if agent_id:
                for operation in ["cancel", "close"]:
                    try:
                        emit("cleanup_" + operation, client.tool_envelope(operation, {"agent_id": agent_id}))
                    except Exception as error:
                        emit("cleanup_error", {"operation": operation, "message": str(error)})
            client.stop()
        (execution / "summary.json").write_text(json.dumps({
            "errors": errors, "business_verdict": "requires_evaluation",
            "transcript": str(transcript),
        }, indent=2) + "\n")
    return int(bool(errors))


if __name__ == "__main__":
    raise SystemExit(main())
