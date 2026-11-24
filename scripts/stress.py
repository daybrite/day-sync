#!/usr/bin/env python3
# Copyright © The Daybrite Project
# SPDX-License-Identifier: MPL-2.0
"""Replayable synthetic sync campaigns; never connects to a real sync account.

Build once, run the test executable directly, and bound each seed's wall time.
Killing cargo alone can leave its test child alive; this runner owns that child.
Failed seeds retain their databases and operation traces below the output directory.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
TEST = "seeded_restart_and_transport_schedules_match_original_operation_replay"


def positive(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--seeds", type=positive, default=32)
    parser.add_argument("--steps", type=positive, default=512)
    parser.add_argument("--timeout", type=positive, default=300, help="seconds per seed")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.seed < 0 or args.seed + args.seeds > 2**64 - 1:
        parser.error("seed range must fit a Rust u64")
    root = args.output or ROOT / "target" / "stress"
    root.mkdir(parents=True, exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix="campaign-", dir=root)).resolve()
    print(f"Evidence: {output}", flush=True)
    build = subprocess.run(
        ["cargo", "test", "--offline", "-p", "day-sync", "--test", "faults",
         "--no-run", "--message-format=json"],
        cwd=ROOT, text=True, capture_output=True, check=False,
    )
    (output / "build.log").write_text(build.stdout + build.stderr)
    if build.returncode:
        print(build.stderr)
        return build.returncode
    binaries = []
    for line in build.stdout.splitlines():
        message = json.loads(line)
        if message.get("reason") == "compiler-artifact" and message.get("executable"):
            binaries.append(message["executable"])
    if len(binaries) != 1:
        raise RuntimeError(f"Expected one test executable, got {binaries}")
    metadata = {
        "seed": args.seed, "seeds": args.seeds, "steps": args.steps,
        "timeout_seconds": args.timeout, "test": TEST,
        "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "status": subprocess.check_output(["git", "status", "--short"], cwd=ROOT, text=True),
    }
    (output / "campaign.json").write_text(json.dumps(metadata, indent=2) + "\n")
    # The normal development case is an uncommitted fix. A revision alone cannot replay it.
    (output / "source.patch").write_bytes(subprocess.check_output(
        ["git", "diff", "--binary", "HEAD"], cwd=ROOT))
    for relative in ("scripts/stress.py", "crates/day-sync/tests/support/stress.rs", "Cargo.lock"):
        destination = output / "source" / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, destination)
    failures = 0
    with (output / "results.jsonl").open("w") as results:
        for seed in range(args.seed, args.seed + args.seeds):
            env = os.environ.copy()
            env.update(DAY_SYNC_STRESS_SEED=str(seed), DAY_SYNC_STRESS_SEEDS="1",
                       DAY_SYNC_STRESS_STEPS=str(args.steps),
                       DAY_SYNC_STRESS_ARTIFACTS=str(output), RUST_BACKTRACE="1")
            start = time.monotonic()
            with (output / f"seed-{seed}.log").open("w") as log:
                try:
                    run = subprocess.run([binaries[0], TEST, "--exact", "--nocapture"],
                                         cwd=ROOT, env=env, stdout=log,
                                         stderr=subprocess.STDOUT, timeout=args.timeout,
                                         check=False)
                    outcome = "passed" if run.returncode == 0 else "failed"
                except subprocess.TimeoutExpired:
                    outcome = "timeout"
            result = {"seed": seed, "steps": args.steps, "outcome": outcome,
                      "seconds": round(time.monotonic() - start, 3)}
            failures += outcome != "passed"
            results.write(json.dumps(result) + "\n")
            results.flush()
            print(json.dumps(result), flush=True)
    print(f"{args.seeds - failures}/{args.seeds} seeds passed; evidence: {output}")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
