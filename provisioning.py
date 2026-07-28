"""
provisioning.py — MT5 terminal lifecycle management for OmniRoute.

Each account (master watcher or slave executor) gets its own portable copy of
the MT5 terminal so sessions are fully isolated — no account-switching via
mt5.login() across a shared process.

Directory layout:
  C:\MT5-Slaves\{account_id}\   — portable terminal copy
    terminal64.exe
    MQL5\
    ...
  C:\Program Files\MetaTrader 5\  — golden image (never modified)

The golden image is copied once per account. Subsequent restarts just re-launch
the existing copy; no re-copy unless the folder is missing.
"""

import asyncio
import logging
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger("provisioning")

GOLDEN_MT5_PATH = Path(r"C:\Program Files\MetaTrader 5")
SLAVES_DIR      = Path(r"C:\MT5-Slaves")

# Track launched processes in memory so we can terminate them on deprovision.
# {account_id: subprocess.Popen}
_processes: dict[str, subprocess.Popen] = {}


def terminal_dir(account_id: str) -> Path:
    return SLAVES_DIR / account_id


def terminal_exe(account_id: str) -> Path:
    return terminal_dir(account_id) / "terminal64.exe"


def is_provisioned(account_id: str) -> bool:
    return terminal_exe(account_id).exists()


def copy_golden_image(account_id: str) -> Path:
    """
    Copy the golden MT5 install to the account's dedicated folder.
    Idempotent — skips copy if folder already exists.
    Returns the terminal directory path.
    """
    dest = terminal_dir(account_id)
    if dest.exists():
        logger.info(f"[provision] {account_id}: terminal folder already exists, skipping copy")
        return dest

    if not GOLDEN_MT5_PATH.exists():
        raise RuntimeError(
            f"Golden MT5 path not found: {GOLDEN_MT5_PATH}. "
            "Install MetaTrader 5 at the default location first."
        )

    SLAVES_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"[provision] {account_id}: copying golden image {GOLDEN_MT5_PATH} → {dest} ...")
    shutil.copytree(str(GOLDEN_MT5_PATH), str(dest))
    logger.info(f"[provision] {account_id}: copy complete")
    return dest


def launch_terminal(
    account_id: str,
    login: int = 0,
    password: str = "",
    server: str = "",
) -> subprocess.Popen:
    """
    Launch the account's terminal in portable mode.
    Passing login/password/server suppresses the MT5 login dialog and lets
    the terminal authenticate automatically so the IPC dispatcher starts
    without any manual interaction on the VPS.
    """
    exe = terminal_exe(account_id)
    if not exe.exists():
        raise RuntimeError(f"Terminal not provisioned for {account_id}: {exe}")

    cmd = [str(exe), "/portable"]
    if login and password and server:
        cmd += [f"/login:{login}", f"/password:{password}", f"/server:{server}"]
        logger.info(f"[provision] {account_id}: launching with auto-login login={login} server={server}")
    else:
        logger.info(f"[provision] {account_id}: launching without auto-login (no credentials provided)")

    proc = subprocess.Popen(cmd, cwd=str(terminal_dir(account_id)))
    _processes[account_id] = proc
    logger.info(f"[provision] {account_id}: terminal PID {proc.pid}")
    return proc


def terminate_terminal(account_id: str):
    """Terminate the account's terminal process if running."""
    proc = _processes.pop(account_id, None)
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        logger.info(f"[provision] {account_id}: terminal terminated")


async def wait_for_ipc(account_id: str, login: int, password: str, server: str,
                        timeout_seconds: int = 120) -> bool:
    """
    Poll mt5.initialize() until IPC is ready and login succeeds.
    Runs blocking MT5 calls in a thread executor so the event loop stays free.
    Returns True on success, False on timeout.
    """
    try:
        import MetaTrader5 as mt5
    except ImportError:
        logger.warning(f"[provision] {account_id}: MetaTrader5 not available — simulation mode")
        return True

    exe_path = str(terminal_exe(account_id))
    loop = asyncio.get_event_loop()
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        def _try_connect():
            if mt5.initialize(path=exe_path, login=login, password=password,
                              server=server, timeout=10_000):
                info = mt5.account_info()
                mt5.shutdown()
                return info is not None and info.login == login
            mt5.shutdown()
            return False

        try:
            ok = await loop.run_in_executor(None, _try_connect)
            if ok:
                logger.info(f"[provision] {account_id}: IPC ready, login verified")
                return True
        except Exception as exc:
            logger.debug(f"[provision] {account_id}: IPC not ready yet ({exc})")

        await asyncio.sleep(5)

    logger.error(f"[provision] {account_id}: IPC timed out after {timeout_seconds}s")
    return False


async def provision_account(
    account_id: str,
    login: int,
    password: str,
    server: str,
    role: str = "slave",  # "master_watcher" or "slave"
) -> dict:
    """
    Provisioning flow:
      1. Copy golden image to C:\\MT5-Slaves\\{account_id}\\ (idempotent)
      2. Launch terminal in portable mode
      3. Sleep briefly so the terminal window can open

    IPC readiness is NOT verified here — the caller (watcher for masters,
    worker loop for slaves) retries via mt5.initialize(path=...) under the
    shared _mt5_lock. This keeps MT5 singleton access serialised.
    """
    try:
        # Run the blocking copy in a thread so the asyncio event loop stays responsive.
        # shutil.copytree on a 500MB+ MT5 install can take 2-3 minutes.
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, copy_golden_image, account_id)
        launch_terminal(account_id, login=login, password=password, server=server)
        # Wait for the terminal to start and connect to the broker.
        # First launch takes ~30s (broker handshake + data download); subsequent launches are faster.
        await asyncio.sleep(30)
        path = str(terminal_exe(account_id))
        return {"status": "launched", "terminal_path": path, "account_id": account_id}
    except Exception as exc:
        logger.error(f"[provision] {account_id}: provisioning failed: {exc}")
        return {"status": "error", "account_id": account_id, "error": str(exc)}


def deprovision_account(account_id: str, delete_folder: bool = False):
    """
    Shut down the terminal process for this account.
    Folder is retained by default (keeps logs, audit trail).
    """
    terminate_terminal(account_id)
    if delete_folder:
        dest = terminal_dir(account_id)
        if dest.exists():
            shutil.rmtree(str(dest), ignore_errors=True)
            logger.info(f"[provision] {account_id}: folder deleted")


def get_running_pids() -> dict:
    """Return {account_id: pid} for all tracked live terminal processes."""
    return {
        aid: proc.pid
        for aid, proc in _processes.items()
        if proc.poll() is None
    }
