"""Run the mid_project_car pipeline.

This project is built around long-running jobs (Kafka producer + Spark Structured Streaming).
A traditional "run script A, wait until it finishes, then run B" doesn't work because these
jobs typically run forever.

This runner supports two practical modes:

- default: start all steps in order (generator -> enrichment -> detection -> counter)
           and keep them running together.
- --sequential: run each step and wait; when you press Ctrl+C, the current step is
                interrupted and the runner proceeds to the next step.

Run from repo root:
  /bin/python3 mid_project_car/run_pipeline.py

Or from this folder:
  cd mid_project_car && /bin/python3 run_pipeline.py
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Step:
    name: str
    script: str


STEPS: list[Step] = [
    Step("data_generator", "data_generator.py"),
    Step("data_enrichment", "data_enrichment.py"),
    Step("alert_detection", "alert_detection.py"),
    Step("alert_counter", "alert_counter.py"),
]


def _project_dir() -> Path:
    return Path(__file__).resolve().parent


def _start_step(step: Step, cwd: Path) -> subprocess.Popen:
    cmd = [sys.executable, step.script]
    env = os.environ.copy()

    print(f"[runner] Starting {step.name}: {' '.join(cmd)} (cwd={cwd})", flush=True)
    # Inherit stdio so logs look the same as running the script directly.
    return subprocess.Popen(cmd, cwd=str(cwd), env=env)


def _interrupt_process(proc: subprocess.Popen, timeout_seconds: float = 15.0) -> None:
    if proc.poll() is not None:
        return

    try:
        proc.send_signal(signal.SIGINT)
    except Exception:
        return

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return
        time.sleep(0.25)

    try:
        proc.terminate()
    except Exception:
        pass


def _terminate_process(proc: subprocess.Popen, timeout_seconds: float = 10.0) -> None:
    if proc.poll() is not None:
        return

    try:
        proc.terminate()
    except Exception:
        return

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return
        time.sleep(0.25)

    try:
        proc.kill()
    except Exception:
        pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run mid_project_car pipeline steps")
    parser.add_argument(
        "--sequential",
        action="store_true",
        help=(
            "Run one step at a time. Ctrl+C interrupts the current step and continues to the next. "
            "(Note: these jobs usually run forever.)"
        ),
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=2.0,
        help="Seconds to wait between starting steps (default: 2.0)",
    )
    parser.add_argument(
        "--from-step",
        choices=[s.name for s in STEPS],
        help="Start from this step name (useful for debugging)",
    )

    args = parser.parse_args(argv)
    cwd = _project_dir()

    steps = STEPS
    if args.from_step:
        idx = next(i for i, s in enumerate(STEPS) if s.name == args.from_step)
        steps = STEPS[idx:]

    if args.sequential:
        for step in steps:
            proc = _start_step(step, cwd=cwd)
            try:
                rc = proc.wait()
            except KeyboardInterrupt:
                print(f"\n[runner] Ctrl+C: interrupting {step.name} and continuing...", flush=True)
                _interrupt_process(proc)
                continue

            if rc != 0:
                print(f"[runner] Step {step.name} exited with code {rc}; stopping.", flush=True)
                return rc

            if args.delay_seconds > 0:
                time.sleep(args.delay_seconds)

        print("[runner] All steps completed.", flush=True)
        return 0

    # Default: start all steps in order and keep them running.
    procs: list[tuple[Step, subprocess.Popen]] = []
    try:
        for step in steps:
            proc = _start_step(step, cwd=cwd)
            procs.append((step, proc))
            if args.delay_seconds > 0:
                time.sleep(args.delay_seconds)

        print("[runner] All steps started. Press Ctrl+C to stop them.", flush=True)

        # Wait for any process to exit; if one fails, stop everything.
        while True:
            for step, proc in procs:
                rc = proc.poll()
                if rc is None:
                    continue
                if rc == 0:
                    print(f"[runner] {step.name} exited normally (code 0).", flush=True)
                else:
                    print(f"[runner] {step.name} exited with code {rc}.", flush=True)

                # Stop the rest.
                for _, other in procs:
                    if other is not proc:
                        _terminate_process(other)
                return rc

            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\n[runner] Ctrl+C: stopping all steps...", flush=True)
        # Try graceful stop first.
        for _, proc in procs:
            _interrupt_process(proc)
        for _, proc in procs:
            _terminate_process(proc)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
