"""Keyed phone identity and AES-GCM storage for PII fields.

An Indian mobile is a small space. A raw SHA-256 can be enumerated, so the
join key is an HMAC. Reversible copies are AES-GCM and are decrypted only
while writing the masked phone onto a report.

AU_PII_KEY must be a 32-byte key, hex or standard base64. A key is never
generated here. The database stores only a SHA-256 fingerprint of that key.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import re

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from au_pipeline.errors import PipelineError

_HEX_KEY = re.compile(r"[0-9a-fA-F]{64}")


def decode_key(text: str) -> bytes:
    """Return 32 key bytes. Reject passphrases and other lengths."""
    token = text.strip()
    if _HEX_KEY.fullmatch(token):
        return bytes.fromhex(token)
    try:
        raw = base64.b64decode(token, validate=True)
    except Exception:
        raw = b""
    if len(raw) == 32:
        return raw
    raise PipelineError(
        "AU_PII_KEY must be a 32-byte key encoded as 64 hex characters or standard base64. "
        'Generate one with: python -c "import os; print(os.urandom(32).hex())"'
    )


def load_raw_key() -> bytes:
    configured = os.environ.get("AU_PII_KEY", "")
    if not configured.strip():
        raise PipelineError(
            "AU_PII_KEY is required. Provide a 32-byte key as 64 hex characters or standard base64. "
            "A replacement key is never generated."
        )
    return decode_key(configured)


def derived_keys(secret: bytes) -> tuple[bytes, bytes]:
    aes_key = hashlib.sha256(b"au-aes-v1\0" + secret).digest()
    mac_key = hashlib.sha256(b"au-hmac-v1\0" + secret).digest()
    return aes_key, mac_key


def phone_hmac(mac_key: bytes, canonical_phone: str) -> str:
    return hmac.new(mac_key, canonical_phone.encode("utf-8"), hashlib.sha256).hexdigest()


def encrypt(aes_key: bytes, plaintext: str) -> tuple[bytes, bytes]:
    nonce = os.urandom(12)
    ciphertext = AESGCM(aes_key).encrypt(nonce, plaintext.encode("utf-8"), None)
    return nonce, ciphertext


def decrypt(aes_key: bytes, nonce: bytes, ciphertext: bytes) -> str:
    plaintext = AESGCM(aes_key).decrypt(bytes(nonce), bytes(ciphertext), None)
    return plaintext.decode("utf-8")


def mask_phone(canonical_phone: str) -> str:
    return "+91******" + canonical_phone[-4:]
