"""OpenClaw gateway subprocess lifecycle manager.

Owns spawning, log streaming, readiness polling, and clean teardown of the
openclaw gateway process. The gateway exposes a WebSocket control plane at
ws://127.0.0.1:18789 that agent.py connects to for Phase 2 responses.

This module has no knowledge of the voice pipeline — it only cares whether
the gateway WebSocket is accepting connections.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from typing import Optional

import websockets

logger = logging.getLogger(__name__)

_GATEWAY_URL      = "ws://127.0.0.1:18789"
_POLL_INTERVAL_S  = 0.3   # seconds between readiness probes
_START_TIMEOUT_S  = 15.0  # maximum seconds to wait for the port to open
_STOP_TIMEOUT_S   = 5.0   # seconds before escalating SIGTERM → SIGKILL
_WS_OPEN_TIMEOUT  = 0.5   # seconds per individual probe attempt


class GatewayManager:
    """Manages the OpenClaw gateway subprocess lifecycle.

    start() spawns `<openclaw_cmd> gateway`, streams its stdout/stderr to
    DEBUG logs, and polls ws://127.0.0.1:18789 until it accepts a WebSocket
    connection (or the 15 s deadline passes).

    stop() sends SIGTERM, waits up to 5 s for a clean exit, then escalates
    to SIGKILL if the process is still alive. The log-reader task is
    cancelled after the process exits so its pipe reads complete gracefully.

    If an external gateway is already running when start() is called, the
    spawned child exits immediately (port conflict) but the readiness probe
    still succeeds — start() returns True and stop() is a no-op. This keeps
    the manual-test workflow (run gateway separately, then python main.py)
    transparent.
    """

    def __init__(self, openclaw_cmd: str = "openclaw") -> None:
        """Initialise GatewayManager.

        Args:
            openclaw_cmd: Name or absolute path of the openclaw binary.
                          The subcommand "gateway" is always appended so the
                          full invocation is `<openclaw_cmd> gateway`.
        """
        self._cmd = openclaw_cmd
        self._process: Optional[asyncio.subprocess.Process] = None
        self._log_task: Optional[asyncio.Task] = None

    # ── Public interface ─────────────────────────────────────────────────────

    async def start(self) -> bool:
        """Spawn the gateway and wait for its WebSocket to accept connections.

        Redirects stdout and stderr to asyncio pipes so _read_logs() can
        stream them to the DEBUG log without blocking the event loop. Polls
        every 300 ms with a websockets probe; returns as soon as the probe
        succeeds or 15 s elapses.

        Returns:
            True if the gateway is ready, False if startup failed or timed out.
        """
        logger.info("Spawning gateway: %s gateway", self._cmd)
        try:
            # CREATE_NO_WINDOW prevents the Node.js gateway process from
            # opening its own console window when spawned from a windowed exe.
            kwargs: dict = {}
            if sys.platform == "win32":
                kwargs["creationflags"] = 0x08000000  # CREATE_NO_WINDOW
            self._process = await asyncio.create_subprocess_exec(
                self._cmd, "gateway",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                **kwargs,
            )
            logger.debug("Gateway process spawned (pid=%d)", self._process.pid)
        except FileNotFoundError:
            # Binary not on PATH — the most common misconfiguration.
            logger.error(
                "Gateway binary not found: %r — is openclaw installed and on PATH?",
                self._cmd,
            )
            return False
        except Exception as exc:
            logger.error("Failed to spawn gateway process: %s", exc)
            return False

        # Drain stdout and stderr in the background so they never fill the
        # pipe buffer and block the subprocess's writes. Each line appears in
        # DEBUG logs prefixed with "gateway:".
        self._log_task = asyncio.create_task(
            self._read_logs(), name="gateway_log_reader"
        )

        ready = await self._poll_until_ready()
        if ready:
            logger.info("Gateway ready at %s", _GATEWAY_URL)
        else:
            logger.error(
                "Gateway did not become ready within %.0fs — stopping", _START_TIMEOUT_S
            )
            await self.stop()
        return ready

    async def stop(self) -> None:
        """Shut down the gateway subprocess.

        Sends SIGTERM (TerminateProcess on Windows) and waits up to 5 s for
        a clean exit. Escalates to SIGKILL if the process is still alive.
        The log-reader task is cancelled after the process exits so no reader
        blocks on a pipe that will never produce more data.

        Safe to call multiple times — guards check process state before
        acting, and a second call is a no-op.
        """
        if self._process is None:
            return

        logger.info("Stopping gateway (pid=%d)", self._process.pid)
        try:
            self._process.terminate()
        except ProcessLookupError:
            pass  # Already exited — nothing to send.

        try:
            await asyncio.wait_for(self._process.wait(), timeout=_STOP_TIMEOUT_S)
            logger.info("Gateway exited cleanly (rc=%d)", self._process.returncode)
        except asyncio.TimeoutError:
            logger.warning(
                "Gateway did not exit after %.0fs — sending SIGKILL", _STOP_TIMEOUT_S
            )
            try:
                self._process.kill()
            except ProcessLookupError:
                pass
            try:
                await self._process.wait()
            except Exception:
                pass

        # Cancel log reader only after the process is confirmed dead — avoids
        # leaving the reader blocked on a pipe that will never produce data.
        if self._log_task is not None and not self._log_task.done():
            self._log_task.cancel()
            try:
                await self._log_task
            except asyncio.CancelledError:
                pass

        self._process = None
        self._log_task = None

    # ── Internal helpers ─────────────────────────────────────────────────────

    async def _poll_until_ready(self, timeout: float = _START_TIMEOUT_S) -> bool:
        """Probe the gateway WebSocket every 300 ms until it responds.

        Uses websockets.connect() so the check exercises the actual WebSocket
        upgrade path. An auth rejection (InvalidHandshake) still counts as
        ready — the server is listening even if it rejects anonymous probes.
        Only OSError (ConnectionRefused, OS-level timeout) means the port is
        genuinely not yet open.

        Args:
            timeout: Maximum seconds to wait before returning False.

        Returns:
            True on first successful or auth-rejected connect; False on timeout.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout

        while loop.time() < deadline:
            # Bail early if the process we spawned has already died AND the
            # port is not open (i.e. not covered by an external gateway).
            if (
                self._process is not None
                and self._process.returncode is not None
                and self._process.returncode != 0
            ):
                logger.error(
                    "Gateway process exited (rc=%d) before port became available",
                    self._process.returncode,
                )
                return False

            try:
                async with websockets.connect(
                    _GATEWAY_URL, open_timeout=_WS_OPEN_TIMEOUT
                ):
                    return True  # Clean WebSocket accept — gateway is ready.
            except OSError:
                # TCP-level failure (ConnectionRefused, timeout) — not ready yet.
                pass
            except Exception:
                # Server responded but rejected (auth error, protocol mismatch,
                # etc.) — port IS open, gateway IS ready.
                return True

            await asyncio.sleep(_POLL_INTERVAL_S)

        return False

    async def _read_logs(self) -> None:
        """Stream gateway stdout and stderr to logger.debug.

        Uses asyncio.gather to drain both pipes concurrently — a serial
        reader would deadlock if one pipe's buffer fills while the other is
        being read. Each line is decoded as UTF-8 (invalid bytes replaced)
        and forwarded at DEBUG level with a "gateway:" prefix.
        """
        async def _drain(stream: asyncio.StreamReader, label: str) -> None:
            try:
                async for raw in stream:
                    line = raw.decode("utf-8", errors="replace").rstrip()
                    if line:
                        logger.debug("gateway: %s", line)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("gateway log reader (%s) error: %s", label, exc)

        try:
            drains: list = []
            if self._process and self._process.stdout:
                drains.append(_drain(self._process.stdout, "stdout"))
            if self._process and self._process.stderr:
                drains.append(_drain(self._process.stderr, "stderr"))
            if drains:
                await asyncio.gather(*drains)
        except asyncio.CancelledError:
            logger.debug("Gateway log reader cancelled")
            raise
