"""LLM agent for Simple Voice Interface.

Phase 2 (active): OpenClaw Gateway WebSocket client.

Opens a fresh WebSocket connection per agent turn, performs the Ed25519
device-auth handshake, sends an agent RPC request, and streams response
tokens back through the same AsyncIterator[str] interface used in Phase 1.

The public interface is identical in both phases:
    async def run(self, transcript: str) -> AsyncIterator[str]

state.py, tts.py, audio.py, and main.py are untouched.

Gateway protocol summary (v3):
  1. Gateway → client: connect.challenge  {event, payload: {nonce, ts}}
  2. Client → gateway: connect req        {type, id, method, params{device, auth, …}}
  3. Gateway → client: connect res        {type, id, ok, payload: {type:"hello-ok", …}}
  4. Client → gateway: agent req          {type, id, method:"agent", params{message, …}}
  5. Gateway → client: event stream       payload.stream="assistant" → yield tokens
                                          payload.stream="lifecycle", phase="end" → done
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import sys
import time
import uuid
from pathlib import Path
from typing import AsyncIterator

import websockets
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
    load_pem_private_key,
)

from config import Config, load

logger = logging.getLogger(__name__)

# Spoken when the agent call fails. A plain string rather than a raised
# exception so the voice loop always has something to say.
_ERROR_UTTERANCE = "Sorry, I ran into an issue. Please try again."

# Persistent Ed25519 device key — same key every run so the device ID is stable.
# The Gateway auto-approves local (loopback) connections so no manual pairing step.
_DEVICE_KEY_FILE = Path.home() / ".openclaw" / "voice-device-key.pem"

# Voice interface client identity values sent in the connect params.
# client.id must be one of the GATEWAY_CLIENT_IDS enum values in the OpenClaw schema.
# client.mode must be one of GATEWAY_CLIENT_MODES ("webchat","cli","ui","backend","node").
# Using "cli" / "cli" matches the operator-role pattern used by the openclaw CLI itself.
_CLIENT_ID = "cli"
_CLIENT_MODE = "cli"
_CLIENT_VERSION = "1.0.0"
_ROLE = "operator"
_SCOPES = ["operator.read", "operator.write"]


# ── Device key management ─────────────────────────────────────────────────────

def _base64url_encode(data: bytes) -> str:
    """Base64url without padding, as required by the OpenClaw signing protocol."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _get_device_key() -> Ed25519PrivateKey:
    """Load an existing device keypair from disk, or generate and persist one.

    Keeping the same key across restarts ensures the device ID never changes,
    which matters for the Gateway's pairing and auto-approval state.
    """
    if _DEVICE_KEY_FILE.exists():
        # load_pem_private_key returns a PrivateKeyTypes union; we know it's Ed25519.
        return load_pem_private_key(_DEVICE_KEY_FILE.read_bytes(), password=None)  # type: ignore[return-value]
    privkey = Ed25519PrivateKey.generate()
    _DEVICE_KEY_FILE.parent.mkdir(parents=True, exist_ok=True)
    _DEVICE_KEY_FILE.write_bytes(
        privkey.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())
    )
    logger.info("Generated new device key → %s", _DEVICE_KEY_FILE)
    return privkey


def _build_device_section(privkey: Ed25519PrivateKey, nonce: str, auth_token: str) -> dict:
    """Build the signed device identity block for the connect request.

    Signing format from OpenClaw gateway server source (gateway-cli-CejL4akr.js):
        payload = buildDeviceAuthPayload({
            deviceId, clientId, clientMode, role, scopes, signedAtMs,
            token: connectParams.auth.token ?? connectParams.auth.deviceToken ?? null,
            nonce: providedNonce
        })
    Pipe-delimited: "v2|{deviceId}|{clientId}|{mode}|{role}|{scopes}|{signedAtMs}|{authToken}|{nonce}"
        signature = base64url(ed25519_sign(privkey, utf8(payload)))

    The auth_token field in the signing payload is the gateway auth token sent in
    auth.token — NOT an empty string. This was confirmed from the server validation code.

    Fields in the returned dict:
        id         — SHA-256 hex of raw public key bytes (64-char hex string)
        publicKey  — Base64url of raw 32-byte Ed25519 public key
        signature  — Base64url of 64-byte Ed25519 signature
        signedAt   — Unix time in milliseconds
        nonce      — Challenge nonce echoed back unchanged
    """
    pubkey_raw: bytes = privkey.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    device_id: str = hashlib.sha256(pubkey_raw).hexdigest()
    pubkey_b64url: str = _base64url_encode(pubkey_raw)

    signed_at_ms: int = int(time.time() * 1000)
    scopes_str: str = ",".join(_SCOPES)
    # The server signs with: token = connectParams.auth.token ?? deviceToken ?? null → "" if null.
    # For our plain-token auth, the auth token IS included in the signing payload.
    token: str = auth_token if auth_token else ""
    payload: str = (
        f"v2|{device_id}|{_CLIENT_ID}|{_CLIENT_MODE}|{_ROLE}|{scopes_str}|{signed_at_ms}|{token}|{nonce}"
    )
    sig_b64url: str = _base64url_encode(privkey.sign(payload.encode("utf-8")))

    logger.debug("Device ID prefix: %s…", device_id[:16])
    return {
        "id": device_id,
        "publicKey": pubkey_b64url,
        "signature": sig_b64url,
        "signedAt": signed_at_ms,
        "nonce": nonce,
    }


# ── Agent ─────────────────────────────────────────────────────────────────────

class LLMAgent:
    """OpenClaw Gateway WebSocket client implementing the voice pipeline agent interface.

    Opens one WebSocket connection per turn: connect → authenticate → agent RPC
    → stream tokens → disconnect. The connection lifetime equals one utterance.

    A per-turn (rather than persistent) connection is used because the Gateway's
    loopback handshake latency is negligible (~5ms), and it avoids idle-keepalive
    complexity on the agent side. The heavy latency in Phase 1 came from the
    STT reconnect (300–500ms), which is already solved in stt.py.
    """

    def __init__(self, config: Config) -> None:
        """Initialise LLMAgent.

        Args:
            config: Frozen Config instance from config.load().
        """
        self._ws_url: str = config.llm_ws_url
        self._gateway_token: str = config.openclaw_gateway_token
        # Load (or generate) the device key once at construction time so the
        # first run() call does not pay the file-I/O cost.
        self._privkey: Ed25519PrivateKey = _get_device_key()

    async def run(self, transcript: str) -> AsyncIterator[str]:
        """Connect to the Gateway and stream the agent response.

        Performs the full handshake, sends the agent request, then yields each
        token as it arrives. The WebSocket closes naturally when the lifecycle
        end event is received, or immediately on CancelledError.

        Args:
            transcript: The user's speech-to-text output for this turn.

        Yields:
            Response text tokens in arrival order.
        """
        try:
            async with websockets.connect(self._ws_url) as ws:

                # ── Step 1: receive challenge ─────────────────────────────────
                raw: str = await ws.recv()
                evt: dict = json.loads(raw)
                if evt.get("event") != "connect.challenge":
                    raise ConnectionError(
                        f"Expected connect.challenge, got {evt.get('event')!r}"
                    )
                nonce: str = evt["payload"]["nonce"]
                logger.debug("Challenge nonce received")

                # ── Step 2: send connect request ──────────────────────────────
                req_id = uuid.uuid4().hex[:8]
                platform = "win32" if sys.platform == "win32" else "linux"
                await ws.send(json.dumps({
                    "type": "req",
                    "id": req_id,
                    "method": "connect",
                    "params": {
                        "minProtocol": 3,
                        "maxProtocol": 3,
                        "client": {
                            "id": _CLIENT_ID,
                            "version": _CLIENT_VERSION,
                            "platform": platform,
                            "mode": _CLIENT_MODE,
                        },
                        "role": _ROLE,
                        "scopes": _SCOPES,
                        "caps": [],
                        "commands": [],
                        "permissions": {},
                        "auth": {"token": self._gateway_token},
                        "locale": "en-US",
                        "userAgent": f"openclaw-voice/{_CLIENT_VERSION}",
                        "device": _build_device_section(self._privkey, nonce, self._gateway_token),
                    },
                }))

                # ── Step 3: receive hello-ok ──────────────────────────────────
                raw = await ws.recv()
                resp: dict = json.loads(raw)
                if not resp.get("ok"):
                    raise ConnectionError(
                        f"Gateway connect rejected: {resp.get('error', resp)}"
                    )
                logger.info("Gateway connected")

                # ── Step 4: send agent request ────────────────────────────────
                agent_req_id = uuid.uuid4().hex[:8]
                await ws.send(json.dumps({
                    "type": "req",
                    "id": agent_req_id,
                    "method": "agent",
                    "params": {
                        "message": transcript,
                        # Idempotency key prevents the Gateway from replaying
                        # the same request if we reconnect mid-turn.
                        "idempotencyKey": uuid.uuid4().hex,
                        "thinking": "off",
                        # Target the default agent. The Gateway requires one of
                        # agentId / sessionId / to (phone) — omitting all three
                        # returns "Pass --to, --session-id, or --agent" error.
                        "agentId": "main",
                    },
                }))
                logger.info("Agent request sent: %r", transcript)

                # ── Step 5: stream response events ────────────────────────────
                # We only care about event=="agent" frames; chat/tick/health/res ignored.
                # assistant frames carry: delta (incremental token) + text (cumulative).
                # Yield delta so TTS receives tokens as they arrive, not all at once.
                async for raw in ws:
                    msg: dict = json.loads(raw)
                    if msg.get("type") != "event" or msg.get("event") != "agent":
                        continue
                    payload: dict = msg.get("payload", {})
                    stream: str = payload.get("stream", "")
                    data: dict = payload.get("data", {})

                    if stream == "assistant":
                        # delta is the incremental text fragment for this event.
                        delta: str = data.get("delta", "")
                        if delta:
                            yield delta

                    elif stream == "lifecycle":
                        if data.get("phase") == "end":
                            logger.info("Agent response complete")
                            break

        except asyncio.CancelledError:
            # VoiceStateMachine cancelled this task (interrupt or shutdown).
            # websockets.connect()'s __aexit__ closes the socket cleanly.
            logger.info("Agent task cancelled")
            # Do not re-raise — generator exits cleanly via StopAsyncIteration.

        except Exception as exc:
            logger.error("Agent error: %s", exc)
            yield _ERROR_UTTERANCE


if __name__ == "__main__":
    cfg = load()
    logging.basicConfig(
        level=getattr(logging, cfg.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    async def _test() -> None:
        agent = LLMAgent(cfg)
        print(f"Connecting to {cfg.llm_ws_url} …\n")

        async for token in agent.run("What is the speed of light? One sentence."):
            print(token, end="", flush=True)
        print()

    asyncio.run(_test())
