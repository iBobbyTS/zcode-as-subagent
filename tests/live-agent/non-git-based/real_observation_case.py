#!/usr/bin/env python3
"""Collect replayable real MCP observation facts without deciding progress.

Exit 1 means the transport, protocol, or cleanup flow failed. Exit 0 only means
the requested facts were captured; a human or calling agent evaluates them.
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
SOURCE = Path(__file__).resolve().parent / "fixtures" / "observation"
RELEASE_MCP = ROOT / "target/release/zcode-as-subagent-mcp"
PACKAGED_MCP = ROOT / "npm/native/darwin-arm64/zcode-as-subagent-mcp"


class McpClient:
    def __init__(self, binary: Path, socket: str, execution: Path, emit):
        self.ident = 0
        self.lines: queue.Queue[str | None] = queue.Queue()
        self.emit = emit
        self.stderr = (execution / "mcp-stderr.log").open("a")
        self.process = subprocess.Popen(
            [str(binary)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.stderr,
            text=True,
            env={**os.environ, "ZCODE_AGENTD_SOCKET": socket},
        )

        def read_stdout():
            for line in self.process.stdout:
                self.lines.put(line)
            self.lines.put(None)

        threading.Thread(target=read_stdout, daemon=True).start()

    def rpc(self, method: str, params: dict) -> dict:
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
            if response.get("id") != self.ident:
                if time.monotonic() >= deadline:
                    raise TimeoutError("MCP response deadline")
                continue
            if "error" in response:
                raise RuntimeError(response["error"])
            return response["result"]

    def initialize(self) -> None:
        self.rpc(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "real-observation-case", "version": "1"},
            },
        )
        self.process.stdin.write(
            json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n"
        )
        self.process.stdin.flush()

    def call(self, name: str, arguments: dict) -> dict:
        result = self.rpc(
            "tools/call",
            {"name": "zcode_subagent_" + name, "arguments": arguments},
        )
        if result.get("isError"):
            raise RuntimeError(result.get("content"))
        return result.get("structuredContent") or json.loads(result["content"][0]["text"])

    def stop(self) -> None:
        if self.process.stdin and not self.process.stdin.closed:
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
        self.emit("facade_exit", {"pid": self.process.pid, "exit": self.process.returncode})


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout-sec", type=float, default=180)
    parser.add_argument("--socket")
    parser.add_argument("--mcp-binary", type=Path)
    parser.add_argument(
        "--plist",
        type=Path,
        default=Path.home() / "Library/LaunchAgents/com.zcode-as-subagent.daemon.plist",
    )
    args = parser.parse_args()
    if args.timeout_sec <= 0:
        parser.error("--timeout-sec must be positive")

    execution = create_execution_root("real-observation-")
    repository = materialize(SOURCE, execution)
    print(execution, flush=True)
    transcript = execution / "transcript.jsonl"
    errors: list[dict] = []
    agents: list[str] = []
    client = None

    def emit(kind: str, value) -> None:
        with transcript.open("a") as stream:
            stream.write(
                json.dumps(
                    {
                        "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        "kind": kind,
                        "value": value,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )

    def record_error(stage: str, error: Exception) -> None:
        errors.append({"stage": stage, "error": str(error)})
        emit("error", errors[-1])

    try:
        program = None
        if args.socket:
            socket = args.socket
        else:
            plist = plistlib.loads(args.plist.read_bytes())
            program = plist["ProgramArguments"]
            socket = program[program.index("--socket") + 1]
            service = subprocess.run(
                ["launchctl", "print", f"gui/{os.getuid()}/{plist['Label']}"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            (execution / "launchctl.txt").write_text(service.stdout + service.stderr)
            if service.returncode:
                raise RuntimeError("LaunchAgent is unavailable")

        binary = args.mcp_binary or (RELEASE_MCP if RELEASE_MCP.exists() else PACKAGED_MCP)
        paths = [binary]
        if program:
            paths.extend(Path(path) for path in [program[0], program[program.index("--runtime") + 1]])
        emit(
            "environment",
            {
                "head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                "socket": socket,
                "program": program,
                "hashes": {
                    str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                    for path in paths
                    if path.is_file()
                },
            },
        )

        client = McpClient(binary, socket, execution, emit)
        client.initialize()
        catalog = client.rpc("tools/list", {})
        emit("catalog", catalog)
        status = client.call("status", {})
        emit("status", status)
        names = [tool["name"] for tool in catalog.get("tools", [])]
        if "zcode_subagent_observe" not in names:
            raise RuntimeError("observe tool is absent from MCP catalog")
        observation = status.get("capabilities", {}).get("observation", {})
        if observation.get("protocol") != "zas-observation/1.1":
            raise RuntimeError("status observation protocol mismatch")

        prompt = (
            "Use Glob once to list read-*.txt. Then call Read separately for read-1.txt through "
            "read-6.txt in numeric order. Run Bash `pwd` six separate times. Do not combine or skip "
            "these calls and do not read other paths. Finally report the six short file contents."
        )
        spawned = client.call(
            "spawn",
            {"repository": str(repository), "permission_mode": "plan", "prompt": prompt},
        )
        agent_id = spawned["agent_id"]
        agents.append(agent_id)
        emit("spawn", spawned)
        revision = 0
        snapshot = None
        deadline = time.monotonic() + args.timeout_sec
        disconnected = False
        while time.monotonic() < deadline:
            snapshot = client.call(
                "poll",
                {"agent_id": agent_id, "after_revision": revision, "timeout_ms": 5000},
            )
            revision = snapshot.get("next_revision", revision)
            emit("observe", client.call("observe", {"agent_id": agent_id}))
            if not disconnected:
                client.stop()
                client = McpClient(binary, socket, execution, emit)
                client.initialize()
                emit("reconnected_status", client.call("status", {}))
                disconnected = True
            if snapshot.get("task", {}).get("phase") == "TERMINAL":
                break
        else:
            raise TimeoutError("completion scenario wall-clock limit")
        emit("terminal_observe", client.call("observe", {"agent_id": agent_id}))
        emit("result", client.call("result", {"agent_id": agent_id}))
        emit("close", client.call("close", {"agent_id": agent_id}))
        agents.remove(agent_id)

        cancel_spawn = client.call(
            "spawn",
            {
                "repository": str(repository),
                "permission_mode": "yolo",
                "prompt": "Run Bash `sleep 30`, then report done. Do not perform other work.",
            },
        )
        cancel_id = cancel_spawn["agent_id"]
        agents.append(cancel_id)
        emit("cancel_spawn", cancel_spawn)
        cancel_deadline = time.monotonic() + min(args.timeout_sec, 30)
        while time.monotonic() < cancel_deadline:
            before_cancel = client.call("observe", {"agent_id": cancel_id})
            emit("before_cancel_observe", before_cancel)
            if before_cancel.get("tools"):
                break
            client.call("poll", {"agent_id": cancel_id, "timeout_ms": 1000})
        emit("cancel", client.call("cancel", {"agent_id": cancel_id}))
        cancel_deadline = time.monotonic() + min(args.timeout_sec, 30)
        while time.monotonic() < cancel_deadline:
            cancelled = client.call("poll", {"agent_id": cancel_id, "timeout_ms": 1000})
            if cancelled.get("task", {}).get("phase") == "TERMINAL" and cancelled.get("task", {}).get("resources_reaped"):
                break
        else:
            raise TimeoutError("cancel scenario did not reap within bound")
        emit("cancelled_observe", client.call("observe", {"agent_id": cancel_id}))
        emit("cancelled_result", client.call("result", {"agent_id": cancel_id}))
        emit("cancelled_close", client.call("close", {"agent_id": cancel_id}))
        agents.remove(cancel_id)
    except Exception as error:
        record_error("flow", error)
    finally:
        if client:
            for agent_id in list(agents):
                try:
                    emit("cleanup_cancel", client.call("cancel", {"agent_id": agent_id}))
                    emit("cleanup_close", client.call("close", {"agent_id": agent_id}))
                except Exception as error:
                    record_error("cleanup", error)
            client.stop()
        (execution / "summary.json").write_text(
            json.dumps(
                {
                    "errors": errors,
                    "business_verdict": "requires_evaluation",
                    "transcript": str(transcript),
                },
                indent=2,
            )
            + "\n"
        )
    return int(bool(errors))


if __name__ == "__main__":
    raise SystemExit(main())
