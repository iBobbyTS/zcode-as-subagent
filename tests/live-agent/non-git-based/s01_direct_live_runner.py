#!/usr/bin/env python3
"""Run a direct-live command three times and archive raw process evidence."""
import argparse
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--output-dir", required=True, type=Path)
    p.add_argument("--binary", required=True, type=Path)
    args = p.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(args.binary.read_bytes()).hexdigest()
    command = [sys.executable, str(Path(__file__).with_name('s01_lifecycle_case.py')),
               '--daemon', str(args.binary.resolve())]
    summary = []
    for index in range(1, 4):
        started = time.time()
        result = subprocess.run(command, capture_output=True, text=True, timeout=90)
        ended = time.time()
        prefix = args.output_dir / f"run-{index}"
        (prefix.with_suffix(".stdout")).write_text(result.stdout)
        (prefix.with_suffix(".stderr")).write_text(result.stderr)
        fact = {"run": index, "started_at_epoch": started, "ended_at_epoch": ended,
                "exit_code": result.returncode, "binary": str(args.binary.resolve()),
                "binary_sha256": digest, "stdout": str(prefix.with_suffix('.stdout')),
                "stderr": str(prefix.with_suffix('.stderr'))}
        fact['command'] = command
        if result.stdout.strip():
            fact['lifecycle'] = json.loads(result.stdout)
        (prefix.with_suffix(".json")).write_text(json.dumps(fact, sort_keys=True) + "\n")
        summary.append(fact)
    print(json.dumps({"runs": summary}, sort_keys=True))
    raise SystemExit(0 if all(run['exit_code'] == 0 for run in summary) else 1)


if __name__ == "__main__":
    main()
