#!/usr/bin/env python3
"""
OmniRoute deployment helper.

Common usage:
  python deploy.py install
  python deploy.py start
  python deploy.py status
  python deploy.py stop

Windows uses Task Scheduler so the app keeps running after the shell closes.
Linux/macOS uses nohup with a PID file.
"""

from __future__ import annotations

import argparse
import os
import platform
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
IS_WINDOWS = platform.system().lower() == "windows"
VENV = ROOT / "venv"
PID_FILE = ROOT / "omniroute.pid"
LOG_FILE = ROOT / "omniroute.out.log"
ERR_FILE = ROOT / "omniroute.err.log"
TASK_NAME = "OmniRouteRun"


def run(cmd: list[str], check: bool = True, shell: bool = False) -> subprocess.CompletedProcess:
    print("+", " ".join(cmd))
    return subprocess.run(cmd, cwd=ROOT, check=check, shell=shell)


def python_bin() -> Path:
    return VENV / ("Scripts/python.exe" if IS_WINDOWS else "bin/python")


def create_venv() -> None:
    if python_bin().exists():
        print(f"venv already exists: {VENV}")
        return
    run([sys.executable, "-m", "venv", str(VENV)])


def install_deps() -> None:
    create_venv()
    run([str(python_bin()), "-m", "pip", "install", "--upgrade", "pip"])
    run([str(python_bin()), "-m", "pip", "install", "-r", str(ROOT / "requirements.txt")])


def uvicorn_args(args: argparse.Namespace) -> list[str]:
    cmd = [
        "-m",
        "uvicorn",
        "main:app",
        "--host",
        args.host,
        "--port",
        str(args.port),
    ]
    if not args.lifespan_on:
        cmd += ["--lifespan", "off"]
    return cmd


def write_windows_launcher(args: argparse.Namespace) -> Path:
    launcher = ROOT / "run-omniroute.cmd"
    content = (
        "@echo off\n"
        f"cd /d {ROOT}\n"
        f"{python_bin()} {' '.join(uvicorn_args(args))}\n"
    )
    launcher.write_text(content, encoding="utf-8")
    return launcher


def open_windows_firewall(port: int) -> None:
    rule_name = f"OmniRoute {port}"
    run(
        [
            "netsh",
            "advfirewall",
            "firewall",
            "add",
            "rule",
            f"name={rule_name}",
            "dir=in",
            "action=allow",
            "protocol=TCP",
            f"localport={port}",
        ],
        check=False,
    )


def create_windows_task(args: argparse.Namespace) -> None:
    launcher = write_windows_launcher(args)
    run(
        [
            "schtasks",
            "/Create",
            "/TN",
            TASK_NAME,
            "/SC",
            "ONCE",
            "/ST",
            "23:59",
            "/TR",
            str(launcher),
            "/F",
        ]
    )


def start_windows(args: argparse.Namespace) -> None:
    create_windows_task(args)
    if args.firewall:
        open_windows_firewall(args.port)
    run(["schtasks", "/Run", "/TN", TASK_NAME])
    print(f"Started OmniRoute on http://{args.host}:{args.port}")


def stop_windows(_: argparse.Namespace) -> None:
    run(["schtasks", "/End", "/TN", TASK_NAME], check=False)
    run(["schtasks", "/Delete", "/TN", TASK_NAME, "/F"], check=False)


def status_windows(args: argparse.Namespace) -> None:
    run(["schtasks", "/Query", "/TN", TASK_NAME, "/V", "/FO", "LIST"], check=False)
    run(["cmd", "/c", f"netstat -ano -p TCP | findstr :{args.port}"], check=False)
    print(f"Health URL: http://127.0.0.1:{args.port}/health")


def start_unix(args: argparse.Namespace) -> None:
    cmd = [str(python_bin()), *uvicorn_args(args)]
    with LOG_FILE.open("ab") as out, ERR_FILE.open("ab") as err:
        proc = subprocess.Popen(
            cmd,
            cwd=ROOT,
            stdout=out,
            stderr=err,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
    PID_FILE.write_text(str(proc.pid), encoding="utf-8")
    print(f"Started OmniRoute PID {proc.pid} on http://{args.host}:{args.port}")


def stop_unix(_: argparse.Namespace) -> None:
    if not PID_FILE.exists():
        print("No PID file found.")
        return
    pid = PID_FILE.read_text(encoding="utf-8").strip()
    run(["kill", pid], check=False)
    PID_FILE.unlink(missing_ok=True)


def status_unix(args: argparse.Namespace) -> None:
    if PID_FILE.exists():
        print(f"PID file: {PID_FILE.read_text(encoding='utf-8').strip()}")
    else:
        print("No PID file found.")
    run(["sh", "-c", f"ss -ltnp 2>/dev/null | grep ':{args.port} ' || true"], check=False)
    print(f"Health URL: http://127.0.0.1:{args.port}/health")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Deploy and manage OmniRoute.")
    parser.add_argument("command", choices=["install", "start", "stop", "restart", "status"])
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument(
        "--lifespan-on",
        action="store_true",
        help="Run normal FastAPI startup hooks. Default is off for UI-only reliability.",
    )
    parser.add_argument(
        "--firewall",
        action="store_true",
        help="Windows only: add an inbound firewall rule for the selected port.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.command == "install":
        install_deps()
        return 0

    if not python_bin().exists():
        print("venv is missing. Run: python deploy.py install", file=sys.stderr)
        return 1

    if args.command == "start":
        start_windows(args) if IS_WINDOWS else start_unix(args)
    elif args.command == "stop":
        stop_windows(args) if IS_WINDOWS else stop_unix(args)
    elif args.command == "restart":
        stop_windows(args) if IS_WINDOWS else stop_unix(args)
        start_windows(args) if IS_WINDOWS else start_unix(args)
    elif args.command == "status":
        status_windows(args) if IS_WINDOWS else status_unix(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
