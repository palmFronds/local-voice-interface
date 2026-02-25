"""Finite state machine for Simple Voice Interface.

Owns every asyncio.Task, Queue, and Event in the pipeline. This is the central
coordinator: every component is started here, runs here, and is stopped here.

No component calls another component directly — all communication flows through
the queues and events defined on this class.
"""

from __future__ import annotations

import asyncio
import logging
from enum import Enum
from typing import AsyncIterator, Optional

from audio import AudioController
from stt import StreamingSTT
from agent import LLMAgent
from tts import StreamingTTS
from config import Config

logger = logging.getLogger(__name__)


class ConversationState(Enum):
    LISTENING = "listening"
    THINKING  = "thinking"
    SPEAKING  = "speaking"


class VoiceStateMachine:
    """Central coordinator for the real-time voice pipeline.

    Owns all asyncio.Tasks, asyncio.Queues, and asyncio.Events in the system.
    The only object that creates tasks; the only object that cancels them.
    All five pipeline components (audio, stt, agent, tts, playback) communicate
    exclusively through the queues and events defined here — no component imports
    or calls another directly.

    State transitions follow a strict cancel-before-start order enforced by
    transition_lock. The lock prevents two concurrent transitions from
    interleaving task creation and cancellation, closing the race condition that
    Vocalis (websocket.py) is vulnerable to: its handle_audio() can be called
    concurrently, with the second call clobbering self.current_audio_task before
    the first task has been properly awaited, leaving an orphaned task that runs
    indefinitely with no reference to cancel it.

    States:
        LISTENING — mic capture + STT active; waiting for utterance_event.
        THINKING  — agent generating response; mic is paused (no false interrupts).
        SPEAKING  — TTS + playback + VAD interrupt watcher active.

    Transitions:
        LISTENING  --[utterance_event]---------> THINKING
        THINKING   --[token_queue non-empty]---> SPEAKING
        SPEAKING   --[interrupt_event]---------> LISTENING
        SPEAKING   --[audio_queue empty
                      AND tts_done_event]------> LISTENING
    """

    def __init__(
        self,
        audio: AudioController,
        stt: StreamingSTT,
        agent: LLMAgent,
        tts: StreamingTTS,
        config: Config,
    ) -> None:
        """Initialise VoiceStateMachine.

        Args:
            audio:  AudioController for mic capture, VAD, and playback.
            stt:    StreamingSTT for Deepgram live transcription.
            agent:  LLMAgent for streaming response generation.
            tts:    StreamingTTS for ElevenLabs audio synthesis.
            config: Frozen Config from config.load().
        """
        self._audio = audio
        self._stt = stt
        self._agent = agent
        self._tts = tts
        self._config = config

        # None until the first _transition_to() call — the guard `if self.state == new_state`
        # would short-circuit the initial LISTENING setup if state were pre-set to LISTENING.
        self.state: Optional[ConversationState] = None
        self.active_tasks: list[asyncio.Task] = []
        self.transition_lock = asyncio.Lock()

        # Queues — unbounded; producers never block
        self.mic_queue: asyncio.Queue[bytes] = asyncio.Queue()
        # token_queue carries str tokens during normal operation and a single
        # None sentinel when the agent stream closes (see _run_agent).
        self.token_queue: asyncio.Queue[Optional[str]] = asyncio.Queue()
        self.audio_queue: asyncio.Queue[bytes] = asyncio.Queue()
        self.transcript_queue: asyncio.Queue[str] = asyncio.Queue()

        # Events
        self.interrupt_event = asyncio.Event()
        self.utterance_event = asyncio.Event()
        self.tts_done_event = asyncio.Event()

        # The agent task is tracked separately from active_tasks during THINKING
        # so it is NOT cancelled on the THINKING → SPEAKING transition — it must
        # keep running to feed the TTS stream. It is added to active_tasks inside
        # _start_speaking() so the SPEAKING → LISTENING teardown cancels it.
        self._agent_task: Optional[asyncio.Task] = None

        # Holds the transcript captured during LISTENING for use in _start_thinking.
        # Stored on the instance rather than passed through _transition_to() to keep
        # the transition signature clean; the value is set immediately before the
        # LISTENING → THINKING call and read only inside the lock.
        self._pending_transcript: str = ""

    # ── Public interface ─────────────────────────────────────────────────────

    async def run(self) -> None:
        """Main loop. Start in LISTENING. Run until KeyboardInterrupt.

        Polls state every 50 ms rather than using event-driven asyncio.wait()
        for every condition. 50 ms polling lag is imperceptible in voice (~1/20
        of a spoken word) and far simpler to reason about than nested wait()
        calls that require careful re-arming, cancellation handling, and partial
        wakeup management. The only cost is one await per loop tick.
        """
        logger.info("Voice pipeline starting")
        # Open the Deepgram WebSocket once here and keep it warm for the session.
        # stream() reuses this connection on every LISTENING entry, eliminating the
        # per-turn ~300–500ms handshake that caused the post-interrupt delay.
        await self._stt.connect()
        await self._transition_to(ConversationState.LISTENING)

        try:
            while True:
                await asyncio.sleep(0.05)  # 50 ms polling interval

                if self.state == ConversationState.LISTENING:
                    if self.utterance_event.is_set():
                        self.utterance_event.clear()
                        self._pending_transcript = await self.transcript_queue.get()
                        await self._transition_to(ConversationState.THINKING)

                elif self.state == ConversationState.THINKING:
                    # Non-blocking O(1) check — no get() needed.
                    # Using get_nowait() + put_nowait() would reorder the consumed
                    # token to the back of the queue, silently corrupting TTS input.
                    logger.debug("THINKING: token_queue size=%d", self.token_queue.qsize())
                    if not self.token_queue.empty():
                        await self._transition_to(ConversationState.SPEAKING)

                elif self.state == ConversationState.SPEAKING:
                    if self.interrupt_event.is_set():
                        self.interrupt_event.clear()
                        await self._transition_to(ConversationState.LISTENING)
                    elif self.audio_queue.empty() and self.tts_done_event.is_set():
                        logger.info("NATURAL COMPLETION: audio_queue empty + tts_done")
                        await self._transition_to(ConversationState.LISTENING)

        except (KeyboardInterrupt, asyncio.CancelledError):
            # KeyboardInterrupt arrives when SIGINT reaches the coroutine directly.
            # CancelledError arrives when asyncio.run() cancels the main task on
            # shutdown (Python 3.11 delivers the interrupt as a task cancellation).
            # Both paths reach the same teardown — cancel every owned task cleanly
            # so no "Task was destroyed but it is pending!" warnings appear on exit.
            logger.info("Shutting down cleanly")

            # _agent_task is tracked separately from active_tasks (so it survives
            # the THINKING → SPEAKING transition). Include it explicitly here so
            # shutdown during a mid-turn does not leave an orphaned agent task.
            all_shutdown_tasks = list(self.active_tasks)
            if self._agent_task is not None and not self._agent_task.done():
                if self._agent_task not in all_shutdown_tasks:
                    all_shutdown_tasks.append(self._agent_task)

            for task in all_shutdown_tasks:
                task.cancel()
            for task in all_shutdown_tasks:
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
            await self._stt.disconnect()

    # ── Transition ───────────────────────────────────────────────────────────

    async def _transition_to(self, new_state: ConversationState) -> None:
        """Cancel all running tasks then start the new state's coroutines.

        Enforces a strict cancel-before-start guarantee: every active task is
        cancelled and fully awaited before new tasks are created. This closes
        two classes of bug that Vocalis does not protect against:

          1. Duplicate producers — e.g. two concurrent mic_capture tasks both
             writing to mic_queue, causing garbled audio or doubled VAD signals.

          2. Orphaned tasks — Vocalis's handle_audio() stores only one task ref
             (self.current_audio_task). If called twice concurrently, the first
             ref is overwritten before being awaited, leaving the first task
             running with no handle to cancel it. Our active_tasks list retains
             all task refs until teardown completes.

        transition_lock serialises concurrent transitions. If an interrupt fires
        while THINKING → SPEAKING is in progress, the interrupt's SPEAKING →
        LISTENING call blocks on the lock and only runs after the first transition
        commits. The self.state == new_state guard then makes it a no-op if the
        machine already passed through that state.

        Args:
            new_state: The ConversationState to enter.
        """
        async with self.transition_lock:
            if self.state == new_state:
                return

            logger.info("State: %s → %s", self.state.value if self.state else "none", new_state.value)

            # 1. Signal all active tasks to stop
            for task in self.active_tasks:
                task.cancel()

            # 2. Await every task to full exit — CancelledError is expected and
            #    swallowed; log warnings only for genuinely unexpected exceptions.
            for task in self.active_tasks:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as exc:
                    logger.warning("Task error during teardown: %s", exc)

            # 3. Reset shared state
            self.active_tasks.clear()
            # Do NOT drain mic_queue when entering LISTENING: frames already in
            # the queue from interrupt speech (the user started talking during
            # SPEAKING) should reach the new STT sender immediately. Draining
            # would discard those leading frames and force the user to repeat the
            # first syllable before STT picks up the new utterance.
            # Do drain mic_queue on all other transitions — stale frames from
            # a previous LISTENING period must not reach the VAD interrupt watcher.
            if new_state != ConversationState.LISTENING:
                self._drain_queue(self.mic_queue)
            self._drain_queue(self.audio_queue)
            # token_queue is NOT drained when entering SPEAKING: the agent task
            # is still running independently and may have already enqueued tokens.
            # Draining here would silently discard the leading words of the agent
            # response — the very tokens that triggered this transition.
            if new_state != ConversationState.SPEAKING:
                self._drain_queue(self.token_queue)
            # Drain stale transcripts when re-entering LISTENING. A previous turn's
            # split utterance (Deepgram finalises a mid-sentence pause as two is_final
            # events) can leave the second fragment sitting here. Without this drain,
            # transcript_queue.get() on the next utterance_event returns the stale
            # fragment instead of the user's new utterance, silently corrupting the
            # next turn's input to the agent.
            # Also clear utterance_event here so a leftover set event cannot fire an
            # immediate LISTENING → THINKING before the user has spoken anything new
            # (belt-and-suspenders alongside the clear inside _start_listening()).
            if new_state == ConversationState.LISTENING:
                self._drain_queue(self.transcript_queue)
                self.utterance_event.clear()
            self.interrupt_event.clear()

            # 4. Commit the new state before starting tasks so any logging or
            #    error path during startup reflects the intended state correctly.
            self.state = new_state

            # 5. Start the new state's coroutines
            if new_state == ConversationState.LISTENING:
                self._start_listening()
            elif new_state == ConversationState.THINKING:
                self._start_thinking(self._pending_transcript)
            elif new_state == ConversationState.SPEAKING:
                self._start_speaking()

    # ── State entry methods ──────────────────────────────────────────────────

    def _start_listening(self) -> None:
        """Start mic capture and STT for the LISTENING state."""
        # Clear utterance_event before starting: a leftover set event from the
        # previous turn would cause an immediate spurious LISTENING → THINKING.
        self.utterance_event.clear()
        capture_task = asyncio.create_task(
            self._audio.start_capture(self.mic_queue),
            name="mic_capture",
        )
        stt_task = asyncio.create_task(
            self._run_stt(),
            name="stt_stream",
        )
        self.active_tasks.extend([capture_task, stt_task])
        logger.info("Listening...")

    def _start_thinking(self, transcript: str) -> None:
        """Start the agent task for the THINKING state.

        The agent task is intentionally NOT added to active_tasks here — see
        class docstring and _start_speaking() for the reasoning.
        """
        self.tts_done_event.clear()
        self._agent_task = asyncio.create_task(
            self._run_agent(transcript),
            name="agent",
        )
        logger.info("Thinking: %r", transcript)

    def _start_speaking(self) -> None:
        """Start TTS synthesis, audio playback, VAD watcher, and mic capture."""
        # Restart mic capture: _watch_for_interrupt() needs frames from mic_queue.
        # STT is not started — raw PCM frames are all VAD requires; no transcription.
        capture_task = asyncio.create_task(
            self._audio.start_capture(self.mic_queue),
            name="mic_capture_speaking",
        )
        tts_task = asyncio.create_task(self._run_tts(), name="tts")
        playback_task = asyncio.create_task(
            self._audio.play(self.audio_queue, self.interrupt_event),
            name="playback",
        )
        vad_task = asyncio.create_task(
            self._watch_for_interrupt(),
            name="vad_interrupt",
        )
        logger.debug("VAD interrupt watcher starting with 1.5s delay")
        self.active_tasks.extend([capture_task, tts_task, playback_task, vad_task])
        # Add the agent task now so the SPEAKING → LISTENING teardown cancels it.
        # If the agent finished naturally before we reach this point, done() is True
        # and task.cancel() is a no-op, so adding it unconditionally is safe.
        if self._agent_task is not None:
            self.active_tasks.append(self._agent_task)
        logger.info("Speaking...")

    # ── Internal runner coroutines ───────────────────────────────────────────

    async def _run_stt(self) -> None:
        """Wrap stt.stream() and put each final transcript onto transcript_queue.

        stt.stream() sets utterance_event before each yield. The main loop sees
        the event, clears it, then drains transcript_queue. Separating the signal
        (utterance_event) from the data (transcript_queue) avoids coupling the STT
        and FSM layers with a combined 'utterance + transcript' object.
        """
        try:
            async for transcript in self._stt.stream(self.mic_queue, self.utterance_event):
                await self.transcript_queue.put(transcript)
        except asyncio.CancelledError:
            logger.info("STT runner cancelled")
            raise
        except Exception as exc:
            logger.warning("STT runner error: %s", exc)

    async def _run_agent(self, transcript: str) -> None:
        """Iterate agent.run() and forward each token to token_queue.

        Sends a None sentinel after the token stream ends so _run_tts() knows
        to stop consuming — even when the agent task is cancelled mid-stream.
        Sets tts_done_event so the main loop can detect natural SPEAKING completion.
        """
        try:
            async for token in self._agent.run(transcript):
                await self.token_queue.put(token)
        except asyncio.CancelledError:
            logger.info("Agent runner cancelled")
            # Do not re-raise: on SPEAKING → LISTENING teardown, the agent task
            # is cancelled and we want it to exit cleanly. Propagating CancelledError
            # here would surface as an unexpected exception in the teardown loop.
        except Exception as exc:
            logger.warning("Agent runner error: %s", exc)
        finally:
            # Send the sentinel even on cancellation so _run_tts() is never left
            # blocked on token_queue.get() waiting for a token that will never arrive.
            # tts_done_event is set by _run_tts() after all audio is produced — not here.
            # The agent may finish generating tokens while ElevenLabs is still synthesizing,
            # so setting it here would cause a premature natural-completion transition.
            await self.token_queue.put(None)

    async def _run_tts(self) -> None:
        """Bridge token_queue into tts.synthesize() and put PCM chunks into audio_queue.

        Creates a local async generator (_token_source) that adapts token_queue's
        Queue interface into the AsyncIterator[str] contract that StreamingTTS
        expects. The generator stops cleanly on the None sentinel without requiring
        any external cancellation or shared flag.
        """
        try:
            async def _token_source() -> AsyncIterator[str]:
                while True:
                    token: Optional[str] = await self.token_queue.get()
                    if token is None:  # Sentinel — agent stream has closed
                        break
                    yield token

            async for chunk in self._tts.synthesize(_token_source()):
                await self.audio_queue.put(chunk)

            # All audio chunks are now in audio_queue. Signal natural completion here
            # rather than in _run_agent() because the agent finishing tokens does not
            # mean ElevenLabs has finished synthesizing — the API call may still be
            # in flight. Setting too early caused a ~55ms premature SPEAKING→LISTENING.
            self.tts_done_event.set()

        except asyncio.CancelledError:
            logger.info("TTS runner cancelled")
            raise
        except Exception as exc:
            logger.warning("TTS runner error: %s", exc)

    async def _watch_for_interrupt(self) -> None:
        """Poll mic_queue during SPEAKING and set interrupt_event on speech.

        Runs as a separate task rather than being embedded in the playback loop
        inside AudioController because audio.py must not import or reference this
        state machine. Keeping interrupt detection here maintains the unidirectional
        dependency: state.py calls audio.py, never the reverse. If VAD were inside
        play(), audio.py would need a reference to interrupt_event and knowledge of
        when to activate — coupling a low-level I/O component to FSM state.

        Only active during SPEAKING. VAD is deliberately paused during THINKING to
        prevent background room noise from triggering a false interrupt before the
        agent has produced any audio at all.
        """
        try:
            # Hard delay before any VAD checks: speaker audio bleeds into the mic
            # during the first ~300ms of playback and would immediately trigger a
            # false interrupt. 2.0 seconds gives the speaker time to stabilise
            # and ensures the user has actually heard something before we listen
            # for a barge-in.
            await asyncio.sleep(2.0)
            logger.debug("VAD interrupt watcher active")
            consecutive_speech_frames: int = 0
            while True:
                frame: bytes = await self.mic_queue.get()
                if self._audio.vad_is_speech(frame):
                    consecutive_speech_frames += 1
                    logger.debug("VAD interrupt: speech frame %d/5", consecutive_speech_frames)
                    # Require 5 consecutive speech frames (5 × 20ms = 100ms) before
                    # declaring an interrupt. A single loud noise or mic bleed would
                    # fire the old single-frame check; sustained speech is a much
                    # stronger signal that the user is genuinely trying to barge in.
                    if consecutive_speech_frames >= 5:
                        logger.info("VAD INTERRUPT TRIGGERED after %d consecutive speech frames",
                                    consecutive_speech_frames)
                        self.interrupt_event.set()
                        break  # Stop watching — the FSM will cancel this task momentarily
                else:
                    # Any silence resets the run — the user must speak continuously
                    # for 100ms, not accumulate 5 scattered speech frames.
                    consecutive_speech_frames = 0
        except asyncio.CancelledError:
            logger.info("VAD interrupt watcher cancelled")
            raise
        except Exception as exc:
            logger.warning("VAD watcher error: %s", exc)

    def _drain_queue(self, queue: asyncio.Queue) -> None:
        """Discard all items currently in a queue without blocking.

        Called during transition teardown to flush stale data so the incoming
        state starts with empty queues. Safe to call without await because
        teardown runs inside transition_lock — no producers are active when
        this method executes.
        """
        while True:
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                break
