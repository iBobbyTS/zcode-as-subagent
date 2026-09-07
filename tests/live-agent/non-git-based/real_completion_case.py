#!/usr/bin/env python3
"""Opt-in official ZCode completion evidence flow.

The harness executes a fixed read-only lifecycle and emits facts. It does not
decide whether the task achieved its goal; that remains the agent/human's
acceptance decision. It exits non-zero only for transport/protocol failures.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
CLI = ROOT / "bin" / "zcode-as-subagent.mjs"
SOURCE = Path(__file__).resolve().parent / "fixtures" / "tiny-agent"
sys.path.insert(0, str(Path(__file__).resolve().parent))
from fixture_workspace import create_execution_root, materialize  # noqa: E402


def call(method: str, payload: dict) -> dict:
    process = subprocess.run(
        ["node", str(CLI), method, "--json", json.dumps(payload)],
        cwd=ROOT, text=True, capture_output=True, check=False,
    )
    if process.returncode:
        raise RuntimeError(f"{method}: {process.stdout}{process.stderr}")
    envelope = json.loads(process.stdout)
    return envelope.get("result", envelope)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout-sec", type=float, default=120.0)
    args = parser.parse_args()
    execution = create_execution_root("real-completion-")
    repository = materialize(SOURCE, execution)
    evidence: dict = {
        "flow": "spawn -> poll -> result -> close",
        "repository": str(repository),
        "observations": [],
    }
    agent_id = None
    try:
        spawned = call("spawn", {
            "repository": str(repository),
            "permission_mode": "plan",
            "prompt": "Reply with exactly REAL_ZCODE_COMPLETED and no tool calls.",
        })
        task = spawned.get("task", spawned)
        agent_id = task.get("agent_id")
        if not agent_id:
            raise RuntimeError("spawn response did not contain agent_id")
        evidence["spawn"] = spawned
        revision = int(spawned.get("revision", task.get("revision", 0)))
        deadline = time.monotonic() + args.timeout_sec
        while time.monotonic() < deadline:
            poll = call("poll", {"agent_id": agent_id, "after_revision": revision, "timeout_ms": 5000})
            revision = int(poll.get("next_revision", revision))
            evidence["observations"].append({
                "revision": poll.get("revision"),
                "next_revision": poll.get("next_revision"),
                "task": poll.get("task"),
                "activity": poll.get("activity"),
                "pending_requests": poll.get("pending_requests", []),
                "result_available": poll.get("result_available"),
            })
            if poll.get("task", {}).get("phase") == "TERMINAL":
                break
        evidence["result"] = call("result", {"agent_id": agent_id, "offset": 0, "limit": 1024})
        evidence["close"] = call("close", {"agent_id": agent_id})
        evidence["final_observation"] = evidence["observations"][-1] if evidence["observations"] else None
        print(json.dumps(evidence, indent=2, sort_keys=True))
        return 0
    except (RuntimeError, json.JSONDecodeError, KeyError, ValueError) as error:
        evidence["error"] = {"type": type(error).__name__, "message": str(error)}
        print(json.dumps(evidence, indent=2, sort_keys=True))
        return 1
    finally:
        if agent_id:
            try:
                snapshot = call("poll", {"agent_id": agent_id, "timeout_ms": 0})
                if snapshot.get("task", {}).get("phase") != "TERMINAL":
                    call("cancel", {"agent_id": agent_id})
            except Exception:
                pass
        shutil.rmtree(execution, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
