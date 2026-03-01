"""Audio capture and VAD for Simple Voice Interface.

Bridges sounddevice's C-level callback thread into the asyncio event loop.
Step 3 adds vad_is_speech() to AudioController. Playback (play) is added in a later step.
"""

from __future__ import annotations

import asyncio
import logging
import math
import struct
from typing import Optional

import numpy as np
import sounddevice as sd
import webrtcvad

from config import Config, load
from ui import ui_rms_queue

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
        # Energy gate: reject frames that are too quiet to be speech before
        # handing off to webrtcvad. webrtcvad classifies waveform shape, not
        # amplitude — it will happily label low-level hiss as speech if the
        # pattern looks right. The RMS check catches that before it reaches the
        # VAD, preventing near-silence from accumulating consecutive-frame counts
        # in _watch_for_interrupt().
        if self._rms(audio_frame) < self._config.vad_energy_threshold:
            return False
        return self._vad.is_speech(audio_frame, self._config.sample_rate)

    def _rms(self, frame: bytes) -> float:
        """Calculate RMS energy of a PCM frame.

        Unpacks int16 little-endian samples (the format sounddevice produces at
        16 kHz mono) and returns the root-mean-square amplitude. Used as a fast
        energy gate before the more expensive webrtcvad classifier.

        Args:
            frame: Raw PCM bytes (int16, mono, 16 kHz).

        Returns:
            RMS energy value in the range 0–32768.
        """
        samples = struct.unpack(f"{len(frame) // 2}h", frame)
        return math.sqrt(sum(s * s for s in samples) / len(samples))

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

    async def start_persistent_capture(self, audio_queue: asyncio.Queue) -> None:
        """Open the PortAudio stream once and stream frames for the session lifetime.

        Unlike start_capture(), this method is called once at startup and never
        called again. The stream runs continuously across all state transitions,
        eliminating the 40–100 ms PortAudio re-open gap that occurs on Windows
        when start_capture() is called on every LISTENING entry.

        Frames are delivered to audio_queue on every PortAudio callback regardless
        of the current conversation state. VoiceStateMachine's per-state logic
        (drain/preserve decisions on mic_queue) handles which frames are acted upon.

        Args:
            audio_queue: Queue to receive raw bytes frames for the session lifetime.
        """
        loop = asyncio.get_running_loop()
        self._stop_event = asyncio.Event()

        def _callback(
            indata: np.ndarray,
            frames: int,
            time,
            status: sd.CallbackFlags,
        ) -> None:
            if status:
                logger.warning("Sounddevice capture status: %s", status)
            frame_bytes: bytes = indata.tobytes()
            loop.call_soon_threadsafe(audio_queue.put_nowait, frame_bytes)
            # Push normalised RMS energy for the UI orb speaking-state visualisation.
            # _rms() is pure computation — safe to call from the PortAudio C thread.
            # audioop was removed in Python 3.13; _rms() is the existing equivalent.
            # SimpleQueue.put() is thread-safe, no asyncio bridge needed here.
            rms = self._rms(frame_bytes) / 32768.0
            ui_rms_queue.put(min(rms, 1.0))

        self._stream = sd.InputStream(
            samplerate=self._config.sample_rate,
            channels=1,
            dtype="int16",
            blocksize=self._config.chunk_size,
            callback=_callback,
        )
        self._stream.start()
        logger.info("Persistent mic capture started")

        try:
            await self._stop_event.wait()
        except asyncio.CancelledError:
            logger.debug("Persistent capture cancelled")
            raise
        finally:
            if self._stream is not None:
                self._stream.close()
                self._stream = None

    async def stop_persistent_capture(self) -> None:
        """Stop the persistent mic stream cleanly at shutdown."""
        if self._stream is not None:
            self._stream.close()
            self._stream = None
        if self._stop_event is not None:
            self._stop_event.set()
        logger.info("Persistent mic capture stopped")


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
