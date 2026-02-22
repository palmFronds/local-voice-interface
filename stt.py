"""Streaming STT for Simple Voice Interface.

Wraps Deepgram's streaming WebSocket API behind a clean async generator interface.
Audio frames in (bytes) → final transcript strings out.

One WebSocket connection is opened per call to stream() and closed when
stream() exits, whether that is a clean return, an exception, or cancellation
by the VoiceStateMachine teardown sequence.
"""

from __future__ import annotations

import asyncio
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
            await connection.send_media(frame)
    except asyncio.CancelledError:
        raise  # Expected on VoiceStateMachine teardown — exit cleanly.
    except Exception as exc:
        # Connection may have been closed before the sender was cancelled
        # (e.g. network drop). Log and exit so the sender task does not hang.
        logger.warning("STT sender error: %s", exc)


class StreamingSTT:
    """Consumes raw PCM audio frames and yields final transcript strings.

    Wraps the Deepgram streaming WebSocket API. One WebSocket connection is
    opened per call to stream() and closed when stream() exits — whether that
    is due to clean completion, an exception, or task cancellation.
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
        # stream() call, making misconfiguration obvious at startup.
        self._client = AsyncDeepgramClient(api_key=config.deepgram_api_key)

    async def stream(
        self,
        audio_queue: asyncio.Queue,
        utterance_event: asyncio.Event,
    ) -> AsyncIterator[str]:
        """Consume audio frames from audio_queue and yield final transcript strings.

        Yields final transcripts only. Interim results are consumed internally
        and discarded. Cancellation is handled cleanly.

        Opens a Deepgram streaming WebSocket connection here, not in __init__,
        because the connection lifetime must equal this task's lifetime. Opening
        in __init__ would leave a dangling WebSocket if the VoiceStateMachine
        transitions away before stream() is first called, or if the same
        StreamingSTT instance is reused across multiple conversation turns.

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
        """
        # Bridge between Deepgram's event-callback model and this async generator.
        # Deepgram dispatches async event handlers inside the event loop, so
        # put_nowait is safe — no thread boundary is crossed.
        transcript_queue: asyncio.Queue[str] = asyncio.Queue()
        sender_task: Optional[asyncio.Task] = None

        async def _on_message(result) -> None:
            """Receive a Deepgram result; enqueue only final transcripts."""
            # The MESSAGE event also carries MetadataEvent, UtteranceEndEvent,
            # and SpeechStartedEvent — guard so we only parse transcript results.
            if not isinstance(result, ListenV1ResultsEvent):
                return
            try:
                text: str = result.channel.alternatives[0].transcript.strip()
            except (AttributeError, IndexError):
                return
            # Deepgram sends interim results (is_final=False) frequently for
            # low-latency display use cases. We discard them: only a finalised
            # result carries the full, corrected transcript that the agent should
            # act on. Yielding partials would require state.py to decide whether
            # to process or ignore each one — a concern that belongs here, not there.
            if result.is_final and text:
                transcript_queue.put_nowait(text)

        async def _on_open(_data) -> None:
            logger.info("STT stream connected")

        async def _on_close(_data) -> None:
            logger.debug("STT stream connection closed")

        async def _on_error(error) -> None:
            logger.warning("Deepgram error: %s", error)

        listener_task: Optional[asyncio.Task] = None

        try:
            # listen.v1.connect() is an asynccontextmanager: it opens the WebSocket,
            # yields the AsyncV1SocketClient, then closes the socket on exit.
            # All parameters are strings — the SDK builds the query string internally.
            async with self._client.listen.v1.connect(
                model=self._config.deepgram_model,
                language="en",
                encoding="linear16",
                sample_rate=str(self._config.sample_rate),
                channels="1",
                interim_results="true",
                punctuate="true",
            ) as connection:
                connection.on(EventType.OPEN, _on_open)
                connection.on(EventType.MESSAGE, _on_message)
                connection.on(EventType.CLOSE, _on_close)
                connection.on(EventType.ERROR, _on_error)

                # start_listening() loops over the WebSocket until it closes,
                # emitting events for every message received. It must run as a
                # task — awaiting it inline would block this generator entirely.
                listener_task = asyncio.create_task(connection.start_listening())
                sender_task = asyncio.create_task(_sender(connection, audio_queue))

                while True:
                    transcript = await transcript_queue.get()
                    # Set the event before yielding so the state machine can
                    # begin its LISTENING → THINKING transition concurrently
                    # with this generator being iterated by the listen loop.
                    utterance_event.set()
                    yield transcript

        except asyncio.CancelledError:
            logger.info("STT stream cancelled")
            raise  # Propagate so the VoiceStateMachine teardown is notified.
        except Exception as exc:
            # Network errors, API errors — log and return so the state machine
            # can recover by transitioning back to LISTENING.
            logger.warning("STT stream error: %s", exc)
        finally:
            # Cancel both tasks. The async with already closed the WebSocket, so
            # start_listening() will have exited; sender may still be blocked on
            # audio_queue.get() and needs an explicit cancel to unblock it.
            for task in (t for t in (sender_task, listener_task) if t is not None):
                if not task.done():
                    task.cancel()
            for task in (t for t in (sender_task, listener_task) if t is not None):
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass  # Task exit is secondary to main cleanup path.


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

            logger.info("Test complete.")

    asyncio.run(_test())
