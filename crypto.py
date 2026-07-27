"""
crypto.py — credential security for OmniRoute.

Two responsibilities:
  1. DPAPI envelope encryption for broker passwords stored in SQLite.
     Uses CryptProtectData / CryptUnprotectData with CRYPTPROTECT_LOCAL_MACHINE
     so any process on this machine (Administrator or SYSTEM) can decrypt —
     no per-user key binding.

  2. bcrypt helpers for app-user password hashing / verification.
"""

import base64
import ctypes
import ctypes.wintypes
import logging
import os
from typing import Optional

import bcrypt as _bcrypt

logger = logging.getLogger("crypto")

# ── DPAPI ─────────────────────────────────────────────────────────────────────

_CRYPTPROTECT_LOCAL_MACHINE = 0x4
_CRYPTPROTECT_UI_FORBIDDEN  = 0x1


class _DATA_BLOB(ctypes.Structure):
    _fields_ = [("cbData", ctypes.wintypes.DWORD),
                ("pbData", ctypes.POINTER(ctypes.c_char))]


def _dpapi_available() -> bool:
    return os.name == "nt"


def encrypt_password(plaintext: str) -> str:
    """
    Encrypt a broker password with Windows DPAPI (machine-scope).
    Returns a base64-encoded string safe for SQLite TEXT storage.
    Falls back to a simple base64 encoding on non-Windows (dev/test only).
    """
    if not plaintext:
        return ""
    if not _dpapi_available():
        # Non-Windows fallback — NOT secure, dev/test only
        logger.warning("DPAPI not available (non-Windows). Using base64 fallback.")
        return "b64:" + base64.b64encode(plaintext.encode()).decode()

    data = plaintext.encode("utf-8")
    blob_in = _DATA_BLOB(len(data), ctypes.cast(ctypes.c_char_p(data), ctypes.POINTER(ctypes.c_char)))
    blob_out = _DATA_BLOB()

    flags = _CRYPTPROTECT_LOCAL_MACHINE | _CRYPTPROTECT_UI_FORBIDDEN
    ok = ctypes.windll.crypt32.CryptProtectData(  # type: ignore[attr-defined]
        ctypes.byref(blob_in),
        None,   # description
        None,   # optional entropy
        None,   # reserved
        None,   # prompt struct
        flags,
        ctypes.byref(blob_out),
    )
    if not ok:
        raise RuntimeError(f"CryptProtectData failed: {ctypes.GetLastError()}")

    encrypted = ctypes.string_at(blob_out.pbData, blob_out.cbData)
    ctypes.windll.kernel32.LocalFree(blob_out.pbData)  # type: ignore[attr-defined]
    return "dpapi:" + base64.b64encode(encrypted).decode()


def decrypt_password(stored: str) -> str:
    """
    Decrypt a broker password previously encrypted by encrypt_password().
    Returns the plaintext password. Never log the return value.
    """
    if not stored:
        return ""

    if stored.startswith("b64:"):
        return base64.b64decode(stored[4:]).decode()

    if not stored.startswith("dpapi:"):
        # Plain-text legacy value (migration path — re-encrypt on next save)
        logger.warning("Decrypting plain-text legacy password. Re-save this account to encrypt it.")
        return stored

    if not _dpapi_available():
        raise RuntimeError("Cannot decrypt DPAPI password on non-Windows host")

    encrypted = base64.b64decode(stored[6:])
    blob_in = _DATA_BLOB(len(encrypted), ctypes.cast(ctypes.c_char_p(encrypted), ctypes.POINTER(ctypes.c_char)))
    blob_out = _DATA_BLOB()

    flags = _CRYPTPROTECT_LOCAL_MACHINE | _CRYPTPROTECT_UI_FORBIDDEN
    ok = ctypes.windll.crypt32.CryptUnprotectData(  # type: ignore[attr-defined]
        ctypes.byref(blob_in),
        None,
        None,
        None,
        None,
        flags,
        ctypes.byref(blob_out),
    )
    if not ok:
        raise RuntimeError(f"CryptUnprotectData failed: {ctypes.GetLastError()}")

    plaintext = ctypes.string_at(blob_out.pbData, blob_out.cbData).decode("utf-8")
    ctypes.windll.kernel32.LocalFree(blob_out.pbData)  # type: ignore[attr-defined]
    return plaintext


def is_encrypted(stored: str) -> bool:
    return stored.startswith("dpapi:") or stored.startswith("b64:")


# ── bcrypt app-user passwords ─────────────────────────────────────────────────

_BCRYPT_ROUNDS = 12


def hash_password(password: str) -> str:
    """Hash a plaintext password with bcrypt. Returns the hash string."""
    return _bcrypt.hashpw(password.encode(), _bcrypt.gensalt(rounds=_BCRYPT_ROUNDS)).decode()


def verify_password(password: str, stored_hash: str) -> bool:
    """Return True if password matches the stored bcrypt hash."""
    try:
        if stored_hash.startswith("$2"):
            # bcrypt hash
            return _bcrypt.checkpw(password.encode(), stored_hash.encode())
        # Legacy SHA-256 hash (migration: accept once, then re-hash on next login)
        import hashlib
        return hashlib.sha256(password.encode()).hexdigest() == stored_hash
    except Exception:
        return False


def is_bcrypt_hash(stored_hash: str) -> bool:
    return stored_hash.startswith("$2")
