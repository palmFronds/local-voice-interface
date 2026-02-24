"""Audio capture and VAD for Simple Voice Interface.

Bridges sounddevice's C-level callback thread into the asyncio event loop.
Step 3 adds vad_is_speech() to AudioController. Playback (play) is added in a later step.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

import numpy as np
import sounddevice as sd
import webrtcvad

from config import Config, load

logger = logging.getLogger(__name__)


class AudioController:
    """Manages microphone capture, VAD, and audio playback.

    Step 2 implements mic capture: start_capture() and stop_capture().
    Step 3 adds vad_is_speech(). play() is added in a later step.

    The sounddevice InputStream callback executes in a dedicated PortAudio
    C thread that is entirely outside the asyncio event loop. Every audio
    frame is forwarded to the loop via call_soon_threadsafe — the only
    thread-safe path into asyncio data structures.
    """

    def __init__(self, config: Config) -> None:
        """Initialise AudioController.

        Args:
            config: Frozen Config instance from config.load().
        """
        self._config = config
        self._stream: Optional[sd.InputStream] = None
        # Initialised inside start_capture so it binds to the correct running loop.
        self._stop_event: Optional[asyncio.Event] = None
        # VAD instance configured once at construction; reused for every frame.
        # Aggressiveness is taken from config to keep all tunables out of this file.
        self._vad = webrtcvad.Vad(config.vad_aggressiveness)

    def vad_is_speech(self, audio_frame: bytes) -> bool:
        """Classify a single audio frame as speech or silence.

        Passes the frame to webrtcvad using the sample rate declared in config.
        Returns True if the frame contains speech, False if silence.

        This method is intentionally stateless: it classifies one frame and returns.
        It does NOT track silence duration, run lengths, or utterance boundaries.
        That temporal logic belongs in VoiceStateMachine (state.py), which owns the
        conversation state and is the only place that can decide when a silence run
        is long enough to fire utterance_complete. Mixing that concern here would
        couple a low-level classifier to FSM state — a violation of the architecture.

        Args:
            audio_frame: Raw PCM bytes. Must be exactly 10ms, 20ms, or 30ms of
                         audio at config.sample_rate (int16, mono). At 16 kHz /
                         20ms that is 320 samples = 640 bytes.

        Returns:
            True if speech detected, False if silence.
        """
        result = self._vad.is_speech(audio_frame, self._config.sample_rate)
        # Per-frame classification is extremely noisy (50 frames/sec); DEBUG only
        # so INFO-level logs stay clean during normal operation.
        logger.debug("VAD frame: %s", "SPEECH" if result else "SILENCE")
        return result

    async def start_capture(self, audio_queue: asyncio.Queue) -> None:
        """Begin streaming 20ms PCM frames into audio_queue.

        Opens a sounddevice InputStream configured for mono int16 audio at
        config.sample_rate. Each blocksize-sample frame is converted to bytes
        and placed on audio_queue. Runs until stop_capture() is called or
        this coroutine is cancelled by the VoiceStateMachine teardown sequence.

        Args:
            audio_queue: Queue to receive raw bytes frames.
                         Each frame is config.chunk_size * 2 bytes (int16 samples).
        """
        # get_running_loop() is preferred over get_event_loop() inside async functions:
        # it raises RuntimeError immediately if called outside a running loop instead of
        # silently creating a new one, which would cause call_soon_threadsafe to target
        # the wrong loop and drop every frame.
        loop = asyncio.get_running_loop()

        # Create the stop event here, not in __init__, so it is always bound to the
        # event loop that is actually running this coroutine. If the object is
        # constructed before asyncio.run() the loop reference would be stale.
        self._stop_event = asyncio.Event()

        def _callback(
            indata: np.ndarray,
            frames: int,
            time,               # PortAudio timing CData struct — not needed here
            status: sd.CallbackFlags,
        ) -> None:
            # sounddevice invokes this from a dedicated PortAudio C thread that lives
            # outside the asyncio event loop. asyncio primitives (Queue, Event, etc.)
            # are not thread-safe: calling queue.put_nowait directly here would be a
            # data race on the queue's internal deque. call_soon_threadsafe serialises
            # the put onto the event loop thread, where every other asyncio operation
            # already runs — making the hand-off safe without any explicit locking.
            if status:
                logger.warning("Sounddevice capture status: %s", status)
            frame_bytes: bytes = indata.tobytes()
            loop.call_soon_threadsafe(audio_queue.put_nowait, frame_bytes)

        self._stream = sd.InputStream(
            samplerate=self._config.sample_rate,
            channels=1,
            dtype="int16",
            blocksize=self._config.chunk_size,
            callback=_callback,
        )
        self._stream.start()
        logger.info("Mic capture started")

        try:
            # Suspend here until stop_capture() sets the event or the task is cancelled.
            await self._stop_event.wait()
        except asyncio.CancelledError:
            # CancelledError arrives here when VoiceStateMachine calls task.cancel()
            # during a state transition teardown. It is expected — log and re-raise so
            # the task exits cleanly and the awaiting teardown loop can proceed.
            logger.debug("start_capture cancelled")
            raise
        finally:
            # Close the stream on every exit path — normal, cancelled, or exception.
            # Without this, a task.cancel() teardown leaves the PortAudio callback
            # thread alive and still writing frames to mic_queue. The next call to
            # start_capture() would open a second stream, creating two producers on
            # the same queue and doubling every audio frame into STT and VAD.
            # stop_capture() also closes the stream, but sets self._stream = None first,
            # so the None guard here prevents a double-close on the normal exit path.
            if self._stream is not None:
                self._stream.close()
                self._stream = None

    async def stop_capture(self) -> None:
        """Stop mic stream cleanly.

        Closes the sounddevice stream first (halting the PortAudio callback thread
        so no new frames are produced), then signals start_capture to return.
        Safe to call even if capture is not currently running.
        """
        # Close the stream before setting the event so the callback thread stops
        # producing frames before start_capture exits — avoids a brief window where
        # frames arrive on a queue that nobody is draining any more.
        if self._stream is not None:
            self._stream.close()
            self._stream = None
        if self._stop_event is not None:
            self._stop_event.set()
        logger.info("Mic capture stopped")


    async def play(self, audio_queue: asyncio.Queue, interrupt_event: asyncio.Event) -> None:
        """Drain audio_queue to the speaker until interrupted or cancelled.

        Writes int16 PCM chunks from audio_queue to a sounddevice OutputStream as
        they arrive. Two exit paths:

          - Interrupt: interrupt_event is set by VoiceStateMachine._watch_for_interrupt()
            when speech is detected during playback. Returns immediately on next check.
          - Natural completion: the state machine cancels this task after detecting
            audio_queue empty AND tts_done_event set. CancelledError propagates cleanly.

        In both cases the with-block calls stream.stop() on exit, which flushes the
        PortAudio output buffer — preventing audio pops or cut-off words.

        stream.write() blocks the calling thread until PortAudio accepts the data
        (up to one buffer period, ~20ms at 16kHz). Offloading to run_in_executor
        keeps the asyncio event loop free to service the interrupt check and other
        coroutines while audio is being written.

        Args:
            audio_queue: Queue of raw PCM bytes chunks (int16, mono, 16 kHz).
            interrupt_event: Set by _watch_for_interrupt when user speaks during playback.
        """
        loop = asyncio.get_running_loop()
        try:
            with sd.OutputStream(
                samplerate=self._config.sample_rate,
                channels=1,
                dtype="int16",
            ) as stream:
                logger.info("Playback started")
                while not interrupt_event.is_set():
                    try:
                        chunk: bytes = await asyncio.wait_for(
                            audio_queue.get(), timeout=0.05
                        )
                    except asyncio.TimeoutError:
                        # No chunk yet — loop back and recheck interrupt_event.
                        continue
                    audio_array = np.frombuffer(chunk, dtype=np.int16)
                    await loop.run_in_executor(None, stream.write, audio_array)
                logger.debug("Playback: interrupt detected, flushing buffer")
        except asyncio.CancelledError:
            logger.debug("Playback cancelled")
            raise
        except Exception as exc:
            logger.error("Playback error: %s", exc)
            raise


if __name__ == "__main__":
    cfg = load()
    logging.basicConfig(
        level=getattr(logging, cfg.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    FRAME_COUNT = 250  # 250 frames × 20ms = 5 seconds of audio

    async def _test() -> None:
        controller = AudioController(cfg)
        queue: asyncio.Queue[bytes] = asyncio.Queue()

        # Run start_capture as a background task so this coroutine can
        # concurrently drain the queue and classify frames while capture runs.
        capture_task = asyncio.create_task(controller.start_capture(queue))

        speech_ms = 0
        silence_ms = 0
        # None until the first frame arrives; thereafter True=SPEECH / False=SILENCE.
        current_is_speech: Optional[bool] = None
        run_frames = 0

        for i in range(FRAME_COUNT):
            frame = await queue.get()
            is_speech = controller.vad_is_speech(frame)

            # Log every frame at DEBUG so the tester can see raw classification
            # without flooding INFO output during normal operation.
            logger.debug(
                "Frame %d/%d: %s",
                i + 1,
                FRAME_COUNT,
                "SPEECH" if is_speech else "SILENCE",
            )

            if current_is_speech is None:
                # First frame — open the first run without a "previous run" annotation.
                current_is_speech = is_speech
                run_frames = 1
                logger.info("VAD: %s started", "SPEECH" if is_speech else "SILENCE")
            elif is_speech == current_is_speech:
                run_frames += 1
            else:
                # Classification flipped — close the previous run, open the new one.
                prev_ms = run_frames * cfg.chunk_ms
                prev_label = "speech" if current_is_speech else "silence"
                new_label = "SPEECH" if is_speech else "SILENCE"
                logger.info("VAD: %s started (after %dms %s)", new_label, prev_ms, prev_label)

                # Accumulate totals before switching state.
                if current_is_speech:
                    speech_ms += prev_ms
                else:
                    silence_ms += prev_ms

                current_is_speech = is_speech
                run_frames = 1

        # Flush the final run into totals before printing the summary.
        if current_is_speech is not None:
            final_ms = run_frames * cfg.chunk_ms
            if current_is_speech:
                speech_ms += final_ms
            else:
                silence_ms += final_ms

        await controller.stop_capture()
        await capture_task  # Ensure start_capture exits fully before returning

        logger.info("VAD test complete. %dms speech, %dms silence detected.", speech_ms, silence_ms)

    asyncio.run(_test())
