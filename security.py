"""
Public Transport IoT System — Security Module
AES-256-GCM encrypted + HMAC-signed digital tickets.
Tamper detection built in.
"""

import os, json, base64, hmac, hashlib, logging
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Tuple

log = logging.getLogger("Security")

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    CRYPTO_OK = True
except ImportError:
    CRYPTO_OK = False
    log.warning("cryptography not installed — tickets use HMAC-only mode")


# Master key — in production load from environment variable
_MASTER_KEY = os.environ.get("TICKET_SECRET", "transit-iot-demo-key-32bytes!!").encode()
_AES_KEY = hashlib.sha256(_MASTER_KEY).digest()  # 32 bytes → AES-256
_HMAC_KEY = hashlib.sha256(_MASTER_KEY + b"hmac").digest()


@dataclass
class Ticket:
    ticket_id: str
    passenger_id: str
    route: str
    valid_from: str
    valid_until: str
    fare: float
    issued_at: str


def issue_ticket(passenger_id: str, route: str, fare: float = 25.0) -> Tuple[str, str]:
    """
    Issue an AES-256-GCM encrypted + HMAC-signed ticket.
    Returns (ticket_id, encoded_token).
    """
    now = datetime.utcnow()
    ticket = {
        "ticket_id": f"TKT-{os.urandom(4).hex().upper()}",
        "passenger_id": passenger_id,
        "route": route,
        "valid_from": now.isoformat(),
        "valid_until": (now + timedelta(hours=2)).isoformat(),
        "fare": fare,
        "issued_at": now.isoformat(),
    }
    payload = json.dumps(ticket).encode()

    if CRYPTO_OK:
        nonce = os.urandom(12)  # AES-GCM requires 12-byte nonce
        aesgcm = AESGCM(_AES_KEY)
        ciphertext = aesgcm.encrypt(nonce, payload, None)
        token = base64.urlsafe_b64encode(nonce + ciphertext).decode()
    else:
        # Fallback: base64 + HMAC (no encryption, still tamper-proof)
        sig = hmac.new(_HMAC_KEY, payload, hashlib.sha256).digest()
        token = base64.urlsafe_b64encode(payload + b"." + sig).decode()

    return ticket["ticket_id"], token


def verify_ticket(token: str) -> Tuple[bool, str, dict]:
    """
    Verify and decrypt a ticket token.
    Returns (is_valid, message, ticket_data).
    """
    try:
        raw = base64.urlsafe_b64decode(token.encode())

        if CRYPTO_OK:
            nonce = raw[:12]
            ciphertext = raw[12:]
            aesgcm = AESGCM(_AES_KEY)
            try:
                payload = aesgcm.decrypt(nonce, ciphertext, None)
            except Exception:
                return False, "❌ Tamper detected — AES-GCM authentication failed", {}
        else:
            parts = raw.rsplit(b".", 1)
            if len(parts) != 2: return False, "❌ Invalid token format", {}
            payload, sig = parts
            expected = hmac.new(_HMAC_KEY, payload, hashlib.sha256).digest()
            if not hmac.compare_digest(sig, expected):
                return False, "❌ Tamper detected — HMAC mismatch", {}

        ticket = json.loads(payload.decode())

        # Check expiry
        valid_until = datetime.fromisoformat(ticket["valid_until"])
        if datetime.utcnow() > valid_until:
            return False, f"❌ Ticket expired at {ticket['valid_until']}", ticket

        return True, f"✅ Valid ticket for {ticket['route']} — Passenger {ticket['passenger_id']}", ticket

    except Exception as e:
        return False, f"❌ Verification error: {str(e)}", {}


def tamper_ticket(token: str) -> str:
    """Deliberately corrupt a token to demo tamper detection."""
    raw = bytearray(base64.urlsafe_b64decode(token.encode()))
    raw[15] ^= 0xFF  # flip bits in the ciphertext
    return base64.urlsafe_b64encode(bytes(raw)).decode()
