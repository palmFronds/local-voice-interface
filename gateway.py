"""OpenClaw gateway subprocess lifecycle manager.

Owns spawning, readiness polling, and clean teardown of the openclaw gateway
process. The gateway exposes a WebSocket control plane at ws://127.0.0.1:18789
that agent.py connects to for Phase 2 streaming responses.

This module has no knowledge of the voice pipeline — it only cares whether
TCP port 18789 is accepting connections.
"""

from __future__ import annotations

import asyncio
import logging
from asyncio.subprocess import DEVNULL
from typing import Optional

logger = logging.getLogger(__name__)

_GATEWAY_HOST = "127.0.0.1"
_GATEWAY_PORT = 18789
_POLL_INTERVAL_S = 0.3   # seconds between TCP probe attempts
_START_TIMEOUT_S = 15.0  # total seconds before giving up


class GatewayManager:
    """Manages the OpenClaw gateway subprocess lifecycle.

    start() spawns the process and blocks until the WebSocket port accepts a
    TCP connection (or the 15s deadline passes). stop() sends SIGTERM and is
    intentionally synchronous so Qt teardown callbacks can call it safely.

    If an external gateway is already running when start() is called, the
    spawned child will exit immediately (port conflict), but the TCP probe
    still succeeds — start() returns True and stop() is a no-op (returncode
    is already set). This makes the test workflow (manual gateway + python
    main.py) transparent.
    """

    def __init__(self, openclaw_cmd: str = "openclaw") -> None:
        """Initialise GatewayManager.

        Args:
            openclaw_cmd: Name or absolute path of the openclaw binary.
        """
        self._cmd = openclaw_cmd
        self._process: Optional[asyncio.subprocess.Process] = None
        # Background task that awaits process exit and logs unexpected death.
        self._monitor_task: Optional[asyncio.Task] = None

    # ── Public interface ─────────────────────────────────────────────────────

    async def start(self) -> bool:
        """Spawn the gateway and wait for TCP port 18789 to accept connections.

        Spawns `<openclaw_cmd> gateway` with stdout/stderr discarded, then
        probes the TCP port every 300ms. Returns as soon as the port accepts
        a connection; gives up after 15 seconds.

        Returns:
            True if the gateway is ready, False if startup failed or timed out.
        """
        logger.info("Spawning gateway: %s gateway", self._cmd)
        try:
            self._process = await asyncio.create_subprocess_exec(
                self._cmd, "gateway",
                stdout=DEVNULL,
                stderr=DEVNULL,
            )
            logger.debug("Gateway process spawned (pid=%d)", self._process.pid)
        except FileNotFoundError:
            # Binary not on PATH — common misconfiguration, give a clear message.
            logger.error("Gateway binary not found: %r — is openclaw installed and on PATH?", self._cmd)
            return False
        except Exception as exc:
            logger.error("Failed to spawn gateway process: %s", exc)
            return False

        # Watch for unexpected death in the background (fires after start() returns).
        self._monitor_task = asyncio.create_task(
            self._monitor_process(), name="gateway_monitor"
        )

        # Poll until port accepts or deadline passes.
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _START_TIMEOUT_S

        while loop.time() < deadline:
            # If the subprocess we spawned has already exited with a non-zero code,
            # something went wrong before it could bind the port — stop polling early
            # unless the port is already open (externally managed gateway).
            proc_dead = (
                self._process.returncode is not None
                and self._process.returncode != 0
            )

            if await self._probe_port():
                logger.info("Gateway ready at %s:%d", _GATEWAY_HOST, _GATEWAY_PORT)
                return True

            if proc_dead:
                # Process died AND port is not open — genuine failure.
                logger.error(
                    "Gateway process exited (code=%d) before port became available",
                    self._process.returncode,
                )
                return False

            await asyncio.sleep(_POLL_INTERVAL_S)

        logger.error(
            "Gateway did not become ready within %.0fs — terminating", _START_TIMEOUT_S
        )
        self.stop()
        return False

    def stop(self) -> None:
        """Terminate the gateway subprocess with SIGTERM.

        Synchronous by design — Qt teardown and atexit handlers run outside
        an asyncio loop so we cannot await anything here. SIGTERM gives the
        gateway a chance to flush in-flight state before the OS reaps it;
        SIGKILL would be immediate but potentially leave the gateway's own
        child processes orphaned.

        Safe to call multiple times — the returncode guard prevents a second
        signal to an already-dead process.
        """
        if self._monitor_task is not None and not self._monitor_task.done():
            self._monitor_task.cancel()
            self._monitor_task = None

        if self._process is not None and self._process.returncode is None:
            self._process.terminate()
            logger.info("Gateway terminated (pid=%d)", self._process.pid)

        self._process = None

    # ── Internal helpers ─────────────────────────────────────────────────────

    async def _probe_port(self) -> bool:
        """Attempt a raw TCP connection to the gateway port.

        Opens a connection and closes it immediately — we only need to know
        that the port is listening, not that the WebSocket handshake works.
        Any network error (refused, timeout, OS error) returns False.
        """
        try:
            reader, writer = await asyncio.open_connection(_GATEWAY_HOST, _GATEWAY_PORT)
            writer.close()
            await writer.wait_closed()
            return True
        except (ConnectionRefusedError, OSError):
            return False

    async def _monitor_process(self) -> None:
        """Await process exit and log if it dies after start() returns.

        Runs as a background task for the session. returncode == 0 is a clean
        shutdown (e.g. the user stopped openclaw externally); anything else is
        unexpected and warrants a WARNING so the operator can investigate.
        """
        if self._process is None:
            return
        returncode = await self._process.wait()
        # 0 = clean exit, 143 = SIGTERM (128+15) from our own stop() call — both expected.
        if returncode in (0, 143, -15):
            logger.info("Gateway process exited cleanly (returncode=%d)", returncode)
        else:
            logger.warning(
                "Gateway process died unexpectedly (returncode=%d) — "
                "voice pipeline will lose agent connectivity",
                returncode,
            )
