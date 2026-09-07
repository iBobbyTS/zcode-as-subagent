#!/usr/bin/env python3
"""Dedicated live respond case: edit mode only, covering allow and deny."""
from __future__ import annotations

import argparse
import json
import subprocess
import time
import tempfile
import shutil
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
CLI = ROOT / "bin" / "zas.mjs"


def cli_call(method: str, payload: dict) -> dict:
    proc = subprocess.run(
        ["node", str(CLI), method, "--json", json.dumps(payload)],
        cwd=ROOT, text=True, capture_output=True, check=False,
    )
    if proc.returncode:
        raise RuntimeError(f"{method} failed: {proc.stdout}{proc.stderr}")
    envelope = json.loads(proc.stdout)
    return envelope.get("result", envelope)


def one_cli(repository: Path, decision: str) -> dict:
    tag = f"respond-case-{decision}-{int(time.time() * 1000)}"
    probe = "src/permission_probe.py"
    spawn = cli_call("spawn", {
        "repository": str(repository),
        "prompt": f"Immediately use the terminal Bash tool to execute `python3 {probe}` and report its output. Do not ask questions or request user input.",
        "permission_mode": "edit",
        "write_manifest": ["src"],
    })
    task = spawn.get("task", spawn)
    agent_id = task["agent_id"]
    revision = int(task.get("revision", 0))
    responses = []
    deadline = time.monotonic() + 300
    terminal = None
    while time.monotonic() < deadline:
        poll = cli_call("poll", {"agent_id": agent_id, "after_revision": revision, "timeout_ms": 5000})
        revision = int(poll.get("next_revision", revision))
        for request in poll.get("pending_requests", []):
            if request.get("state") not in ("pending", "sending"):
                continue
            if not request.get("respondable"):
                cli_call("cancel", {"agent_id": agent_id})
                raise RuntimeError(f"{decision}: observed non-respondable pending request: {request.get('kind')}")
            args = {"agent_id": agent_id, "request_id": request["request_id"], "decision": decision,
                    "reason": f"respond case {decision}"}
            first = cli_call("respond", args)
            second = cli_call("respond", args)
            responses.append({"request": request, "first": first, "repeat": second})
        if poll.get("task", {}).get("phase") == "TERMINAL":
            terminal = poll["task"]
            break
        time.sleep(0.25)
    if terminal is None:
        cli_call("cancel", {"agent_id": agent_id})
        raise RuntimeError(f"{decision}: no terminal state")
    result = cli_call("result", {"agent_id": agent_id})
    closed = cli_call("close", {"agent_id": agent_id})
    if not responses:
        raise RuntimeError(f"{decision}: terminal without observed permission request")
    return {"decision": decision, "agent_id": agent_id, "responses": responses,
            "task": terminal, "result": result, "closed": closed}


def run_mcp(repository: Path) -> None:
    proc = subprocess.run(["codex", "exec", "--dangerously-bypass-approvals-and-sandbox", "--json",
        f"Use only zcode_as_subagent MCP in {repository}. Run one edit task: execute Bash `python3 src/permission_probe.py`; poll until a pending permission request is visible and return its agent_id, request_id, and revision as JSON. Do not respond or close."], cwd=ROOT, text=True, capture_output=True, check=False)
    print(proc.stdout)
    if proc.returncode:
        raise RuntimeError(proc.stderr)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--transport", choices=("cli", "mcp"), required=True)
    args = parser.parse_args()
    source = args.repository.resolve()
    execution = Path(tempfile.mkdtemp(prefix="respond-case-", dir=ROOT / "tests/live-agent/workspace"))
    repository = execution / "repository"
    shutil.copytree(source, repository)
    (repository / "src/permission_probe.py").write_text("print('PERMISSION_PROBE_EXECUTED')\n", encoding="utf-8")
    if args.transport == "mcp":
        run_mcp(repository)
        return 0
    output = {decision: one_cli(repository, decision) for decision in ("allow", "deny")}
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
