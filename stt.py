"""Streaming STT for Simple Voice Interface.

Wraps Deepgram's streaming WebSocket API behind a clean async generator interface.
Audio frames in (bytes) → final transcript strings out.

One WebSocket connection is opened at startup (via connect()) and kept warm for
the session lifetime. stream() reuses this connection on every call, eliminating
the ~300–500ms Deepgram handshake that would otherwise occur on each
SPEAKING → LISTENING transition.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import AsyncIterator, Optional

from deepgram import AsyncDeepgramClient
from deepgram.core.events import EventType
from deepgram.extensions.types.sockets import ListenV1ResultsEvent

from config import Config, load

logger = logging.getLogger(__name__)


async def _sender(connection, audio_queue: asyncio.Queue) -> None:
    """Forward PCM frames from the mic queue to an open Deepgram connection.

    Runs as a sibling asyncio.Task alongside the transcript-yield loop inside
    StreamingSTT.stream(). The separation is deliberate: if audio sending were
    inlined inside the generator body (before or after each yield), the generator
    would block between yields and starve Deepgram of audio input entirely.
    Running the sender as a separate task lets both loops advance independently.

    Args:
        connection: Open Deepgram live connection from listen.v1.connect().
        audio_queue: Source of 20ms PCM frames produced by AudioController.
    """
    try:
        while True:
            frame: bytes = await audio_queue.get()
            # Per-frame logging is very noisy (50 frames/sec); DEBUG only so that
            # INFO-level logs are not flooded during normal operation.
            logger.debug("STT: sending frame %d bytes", len(frame))
            await connection.send_media(frame)
    except asyncio.CancelledError:
        raise  # Expected on VoiceStateMachine teardown — exit cleanly.
    except Exception as exc:
        # Connection may have been closed before the sender was cancelled
        # (e.g. network drop). Log and exit so the sender task does not hang.
        logger.warning("STT sender error: %s", exc)


class StreamingSTT:
    """Consumes raw PCM audio frames and yields final transcript strings.

    One Deepgram WebSocket connection is opened at startup via connect() and
    held open for the session lifetime by a keeper task. stream() attaches a
    sender task to the warm connection on each LISTENING entry — eliminating the
    per-turn ~300–500ms TLS + HTTP upgrade handshake that would otherwise fire on
    every SPEAKING → LISTENING transition.
    """

    def __init__(self, config: Config) -> None:
        """Initialise StreamingSTT.

        Args:
            config: Frozen Config instance from config.load().
        """
        self._config = config
        # AsyncDeepgramClient is stateless at construction — it holds only
        # authentication credentials. No network I/O happens here. Creating it
        # once validates the API key format immediately rather than at the first
        # connect() call, making misconfiguration obvious at startup.
        self._client = AsyncDeepgramClient(api_key=config.deepgram_api_key)

        # Persistent connection — set inside _keep_connection_alive(), used by stream().
        self._connection = None
        self._listener_task: Optional[asyncio.Task] = None
        self._keeper_task: Optional[asyncio.Task] = None
        # Set when the connection is open and ready. Cleared on connection drop so
        # stream() can wait for the keeper to reconnect rather than crash.
        self._connection_ready: asyncio.Event = asyncio.Event()
        # Shared across all stream() calls. Drained at the start of each stream()
        # call to flush any results that arrived between LISTENING periods.
        self._transcript_queue: asyncio.Queue[str] = asyncio.Queue()

    async def connect(self) -> None:
        """Open the Deepgram WebSocket and keep it warm for the session.

        Must be called once before stream() is first used. Blocks until the
        connection is confirmed open. The keeper task then maintains it for the
        session lifetime, reconnecting automatically on drops.
        """
        self._keeper_task = asyncio.create_task(
            self._keep_connection_alive(), name="stt_keeper"
        )
        await self._connection_ready.wait()
        logger.info("STT connection ready")

    async def disconnect(self) -> None:
        """Close the persistent Deepgram WebSocket and clean up.

        Called at shutdown. Cancels the keeper task which exits the async with
        block, closing the WebSocket cleanly.
        """
        if self._keeper_task and not self._keeper_task.done():
            self._keeper_task.cancel()
            try:
                await self._keeper_task
            except asyncio.CancelledError:
                pass
        logger.info("STT connection closed")

    async def _keep_connection_alive(self) -> None:
        """Hold the Deepgram WebSocket open for the session lifetime.

        Runs as a long-lived task started by connect(). Opens the WebSocket,
        stores it as self._connection, signals _connection_ready, then awaits
        the listener task — which holds the async with block open until the
        connection closes. On drops, retries with exponential backoff (1s, 2s,
        4s, 8s cap) so transient network issues self-heal without crashing.
        """
        async def _on_open(_data) -> None:
            logger.info("STT stream connected")

        async def _on_close(_data) -> None:
            logger.debug("STT stream connection closed")

        async def _on_error(error) -> None:
            logger.warning("Deepgram error: %s", error)

        attempt = 0
        while True:
            attempt += 1
            # Exponential backoff, capped at 8 seconds. Reset to 0 on a
            # successful connection so a recovered session starts fresh.
            backoff = min(2 ** (attempt - 1), 8.0)

            keepalive_task: Optional[asyncio.Task] = None
            try:
                async with self._client.listen.v1.connect(
                    model=self._config.deepgram_model,
                    language="en",
                    encoding="linear16",
                    sample_rate=str(self._config.sample_rate),
                    channels="1",
                    interim_results="true",
                    punctuate="true",
                ) as connection:
                    self._connection = connection
                    connection.on(EventType.OPEN, _on_open)
                    connection.on(EventType.MESSAGE, self._on_persistent_message)
                    connection.on(EventType.CLOSE, _on_close)
                    connection.on(EventType.ERROR, _on_error)

                    self._listener_task = asyncio.create_task(
                        connection.start_listening(), name="stt_listener"
                    )
                    # Prevent Deepgram's ~10s idle timeout during THINKING/SPEAKING
                    # when no audio frames are being sent via the sender task.
                    keepalive_task = asyncio.create_task(
                        self._send_keepalives(connection), name="stt_keepalive"
                    )
                    # Signal stream() that the connection is ready.
                    self._connection_ready.set()
                    # Reset attempt counter on success so backoff starts over if
                    # the connection later drops after being stable.
                    attempt = 0

                    # Awaiting the listener holds the async with open.
                    # When start_listening() returns (WebSocket closed), we fall
                    # through to the retry logic below.
                    await self._listener_task

            except asyncio.CancelledError:
                # disconnect() was called — exit without retrying.
                logger.info("STT keeper cancelled")
                raise
            except Exception as exc:
                self._connection_ready.clear()
                logger.warning(
                    "STT connection lost, reconnecting in %.1fs (attempt %d): %s",
                    backoff, attempt, exc,
                )
                try:
                    await asyncio.sleep(backoff)
                except asyncio.CancelledError:
                    raise  # Cancelled during backoff — stop retrying.
            finally:
                # Cancel both the listener and keepalive tasks. The async with
                # already closed the WebSocket, so the listener will have exited;
                # the keepalive may still be sleeping and needs an explicit cancel.
                for task in (t for t in (self._listener_task, keepalive_task) if t is not None):
                    if not task.done():
                        task.cancel()
                for task in (t for t in (self._listener_task, keepalive_task) if t is not None):
                    try:
                        await task
                    except (asyncio.CancelledError, Exception):
                        pass

    async def _send_keepalives(self, connection) -> None:
        """Send a KeepAlive message every 8 seconds to prevent Deepgram's idle timeout.

        Deepgram closes connections with error 1011 after ~10 seconds of silence.
        This fires during THINKING and SPEAKING states when no audio is being sent
        via the sender task. 8 seconds gives a 2-second margin below the timeout.
        """
        while True:
            await asyncio.sleep(8)
            try:
                await connection.send(json.dumps({"type": "KeepAlive"}))
                logger.debug("STT keepalive sent")
            except Exception:
                # Connection is gone — stop trying. The keeper's retry logic
                # will re-establish it and start a new keepalive task.
                break

    async def _on_persistent_message(self, result) -> None:
        """Receive a Deepgram result and enqueue only final transcripts.

        Registered once on the persistent connection. Writes to
        self._transcript_queue which is polled by stream() during LISTENING.
        Between LISTENING periods the queue may accumulate entries if Deepgram
        sends results unprompted — these are drained at the start of each
        stream() call so stale results never trigger a spurious utterance.
        """
        if not isinstance(result, ListenV1ResultsEvent):
            return
        try:
            text: str = result.channel.alternatives[0].transcript.strip()
        except (AttributeError, IndexError):
            return
        # Discard interim results — only final, corrected transcripts go upstream.
        if result.is_final and text:
            self._transcript_queue.put_nowait(text)

    async def stream(
        self,
        audio_queue: asyncio.Queue,
        utterance_event: asyncio.Event,
    ) -> AsyncIterator[str]:
        """Consume audio frames from audio_queue and yield final transcript strings.

        Reuses the persistent Deepgram connection opened by connect(). Starts only
        a sender_task for this utterance — the listener and WebSocket stay open
        across calls, so no handshake occurs here.

        Yields final transcripts only. Interim results are filtered in
        _on_persistent_message(). Cancellation is handled cleanly.

        Args:
            audio_queue: Queue of raw PCM bytes frames produced by AudioController.
                         Each frame is config.chunk_size * 2 bytes (int16 mono
                         at config.sample_rate Hz).
            utterance_event: asyncio.Event set when a final transcript is ready.
                             VoiceStateMachine watches this to trigger the
                             LISTENING → THINKING transition. Cleared by the
                             state machine on transition, not here.

        Yields:
            Complete final transcript strings, one per finalised utterance.
            Yields "" once if the connection has dropped and cannot be used.
        """
        # Flush results that arrived between the previous utterance and now
        # (Deepgram may emit events while the connection is idle during SPEAKING).
        # Without this drain, a stale transcript could trigger an instant spurious
        # LISTENING → THINKING transition before the user has spoken anything.
        while not self._transcript_queue.empty():
            self._transcript_queue.get_nowait()

        # If the keeper is reconnecting, wait rather than failing immediately.
        if not self._connection_ready.is_set():
            logger.warning("STT connection not ready — waiting for reconnect")
            await self._connection_ready.wait()

        sender_task: asyncio.Task = asyncio.create_task(
            _sender(self._connection, audio_queue), name="stt_sender"
        )

        try:
            while True:
                # Poll with a short timeout so we detect a silently dropped
                # connection: if listener_task exits (WebSocket closed by the
                # server) while the user is not speaking, transcript_queue.get()
                # would block forever. The timeout lets us check listener health.
                try:
                    transcript = await asyncio.wait_for(
                        self._transcript_queue.get(), timeout=5.0
                    )
                except asyncio.TimeoutError:
                    if self._listener_task and self._listener_task.done():
                        # Connection dropped mid-utterance. The keeper will reconnect,
                        # but we can't continue this stream() call on the dead socket.
                        logger.warning("STT listener exited mid-stream")
                        utterance_event.set()
                        yield ""
                        return
                    continue  # Normal silence pause — keep waiting
                # Set the event before yielding so the state machine can
                # begin its LISTENING → THINKING transition concurrently
                # with this generator being iterated by the listen loop.
                utterance_event.set()
                yield transcript

        except asyncio.CancelledError:
            logger.info("STT stream cancelled")
            raise
        finally:
            # Cancel the sender. The WebSocket connection stays open — only the
            # per-utterance audio feed stops. The keeper handles connection lifetime.
            sender_task.cancel()
            try:
                await sender_task
            except (asyncio.CancelledError, Exception):
                pass


if __name__ == "__main__":
    from audio import AudioController

    cfg = load()
    logging.basicConfig(
        level=getattr(logging, cfg.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    TEST_DURATION_SECS = 10

    async def _test() -> None:
        controller = AudioController(cfg)
        stt = StreamingSTT(cfg)
        mic_queue: asyncio.Queue[bytes] = asyncio.Queue()
        utterance_event = asyncio.Event()

        await stt.connect()
        capture_task = asyncio.create_task(controller.start_capture(mic_queue))

        async def _run_stt() -> None:
            async for transcript in stt.stream(mic_queue, utterance_event):
                logger.info("TRANSCRIPT: %s", transcript)

        stt_task = asyncio.create_task(_run_stt())

        try:
            await asyncio.sleep(TEST_DURATION_SECS)
        finally:
            stt_task.cancel()
            try:
                await stt_task
            except asyncio.CancelledError:
                pass

            await controller.stop_capture()
            capture_task.cancel()
            try:
                await capture_task
            except asyncio.CancelledError:
                pass

            await stt.disconnect()
            logger.info("Test complete.")

    asyncio.run(_test())
