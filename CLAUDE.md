# CLAUDE.md — Simple Voice Interface

This file is the authoritative specification for this project.
Read it completely before writing any code, creating any file, or making any architectural decision.
Every decision you make should be traceable back to something written here.

---

## Project Identity

**Name:** Simple Voice Interface
**Purpose:** A minimal, local, real-time interruptible voice layer for the OpenClaw autonomous agent.
**Runtime:** Local Python CLI application. No browser. No frontend. No hosted infrastructure.
**Entry point:** `python main.py` starts the entire system.

This is not a chatbot UI. It is not a web app. It is a voice pipeline — a direct connection
between a microphone, an autonomous agent, and a speaker — with full-duplex interruption support.

---

## What This Project Is and Is Not

### IS:
- A local Python asyncio application
- A real-time audio pipeline: mic → STT → agent → TTS → speaker
- An interruptible system: the user can speak while the agent is speaking and the agent stops
- A clean abstraction layer over any agent backend (Phase 1: streaming LLM, Phase 2: OpenClaw)
- A portfolio-quality codebase: readable, documented, intentional

### IS NOT:
- A web application
- A REST API server
- A WebSocket server with a browser client
- A framework or library for others to build on
- A wrapper around OpenAI's Realtime API (we build the pipeline ourselves)
- A clone or fork of any existing project

---

## Two-Phase Build Plan

### Phase 1 — Full voice pipeline with streaming LLM stub (BUILD THIS NOW)

`agent.py` wraps a direct streaming LLM API call (OpenAI-compatible).
This is NOT a shortcut — it is the correct way to validate the entire voice pipeline
before introducing OpenClaw's complexity.

The interface contract for `agent.py` is identical in both phases:
```python
async def run(self, transcript: str) -> AsyncIterator[str]
```
The voice pipeline never knows or cares what is behind this method.

### Phase 2 — Swap agent.py to connect to OpenClaw Gateway (DEFERRED)

OpenClaw is a Node.js autonomous agent that runs as a separate daemon process.
It exposes a WebSocket control plane at `ws://127.0.0.1:18789`.
Phase 2 replaces `agent.py` internals with a WebSocket client that:
- Connects to the running OpenClaw Gateway
- Sends the transcript as a structured JSON message
- Receives response tokens over the WebSocket
- Yields them through the same `AsyncIterator[str]` interface

Phase 2 touches ONLY `agent.py`. Everything else — audio, STT, TTS, state machine — stays identical.

**Do not implement Phase 2 until Phase 1 is fully working and tested.**
**Do not reference OpenClaw at all during Phase 1 implementation.**

---

## Architecture Overview

The system is a single async event loop running five concurrent coroutines,
coordinated by an explicit finite state machine.

```
Microphone → [AudioController] → [StreamingSTT] → [LLMAgent] → [StreamingTTS] → [AudioController] → Speaker
                    ↑                                                                      |
                    └─────────────────── interrupt_event (asyncio.Event) ─────────────────┘
                                         fires when VAD detects speech during SPEAKING state
```

No component calls another component directly.
Components communicate exclusively through:
- `asyncio.Queue` — for streaming data (audio chunks, text tokens)
- `asyncio.Event` — for signals (interrupt, utterance complete)
- `asyncio.Task` — for lifecycle management (cancel, await)

The `VoiceStateMachine` in `state.py` owns all tasks and all events.
It is the only thing that starts coroutines and the only thing that cancels them.

---

## Critical Lessons from Reference Research

We studied the Vocalis open-source voice interface project before building this.
These are the specific problems we identified and how we intentionally solve them differently:

**Problem 1 — Vocalis uses blocking LLM calls.**
Vocalis uses `requests.post()` — synchronous and blocking. The user waits in silence
while the entire LLM response generates, then hears it all at once.
Our solution: streaming LLM calls from day one. Tokens flow into TTS as they are generated.
The user hears the first word within ~200ms of the model starting to respond.

**Problem 2 — Vocalis cannot cancel in-flight LLM generation.**
Because Vocalis uses a blocking HTTP call, it cannot stop it on interrupt. It silently
finishes the full LLM call, discards the result, and resets — wasting compute.
Our solution: the agent runs as an `asyncio.Task`. On interrupt, `task.cancel()` fires.
The agent coroutine catches `asyncio.CancelledError` and exits cleanly. True cancellation.

**Problem 3 — Vocalis has no explicit state machine.**
Vocalis embeds state logic across handler functions in one file. There is no single place
where transitions are defined, no transition lock, no protection against concurrent state changes.
Our solution: `VoiceStateMachine` in `state.py` with a `ConversationState` enum,
a `transition_lock`, and a strict cancel-before-start teardown sequence on every transition.

**Problem 4 — Vocalis interrupt only mutes TTS output, not the LLM.**
Vocalis's interrupt sets a flag that stops audio chunks from being sent to the browser.
The LLM keeps running. Only the speaker goes silent. The pipeline is still active.
Our solution: interrupt cancels agent task + TTS task + playback loop, in that order.
The entire pipeline stops, not just the speaker.

---

## The State Machine

This is the most important component. Implement `state.py` before any other file.

### States

```python
class ConversationState(Enum):
    LISTENING = "listening"
    THINKING  = "thinking"
    SPEAKING  = "speaking"
```

### State Definitions

**LISTENING**
- Active coroutines: VAD loop (inside AudioController), StreamingSTT
- Waiting for: end-of-utterance signal from STT
- Transitions to: THINKING when a final transcript is produced
- On entry: start mic capture, start STT stream
- On exit: cancel STT stream, flush mic buffer

**THINKING**
- Active coroutines: LLMAgent task
- Waiting for: first token of agent response
- Transitions to: SPEAKING when agent yields first token
- On entry: pass transcript to agent, begin streaming response
- On exit: do NOT cancel agent — it continues running to feed TTS
- VAD is paused during THINKING (prevent background noise triggering false interrupt)

**SPEAKING**
- Active coroutines: StreamingTTS, playback loop (inside AudioController), VAD (interrupt-watch only)
- Waiting for: either PLAYBACK_COMPLETE or INTERRUPT
- Transitions to: LISTENING on either event
- On entry: start TTS with agent token stream, start playback, reactivate VAD in interrupt mode
- On exit (PLAYBACK_COMPLETE): clean shutdown of TTS and playback
- On exit (INTERRUPT): cancel agent task, cancel TTS, cancel playback, flush audio buffer

### Transitions

```
LISTENING  --[utterance_complete]--> THINKING
THINKING   --[first_token]---------> SPEAKING
SPEAKING   --[playback_complete]---> LISTENING
SPEAKING   --[interrupt_detected]--> LISTENING  ← the critical path
```

### Transition Rules (non-negotiable)

1. Always cancel active tasks BEFORE starting new ones. Never the reverse.
2. Always `await` cancelled tasks to completion before proceeding. Catch `asyncio.CancelledError`.
3. Use a `transition_lock: asyncio.Lock` to prevent concurrent transitions.
   If the lock is held when a second transition arrives, the second is a no-op.
4. The SPEAKING → LISTENING (interrupt) path must flush the audio output buffer
   before releasing the lock, to prevent audio pops or cut-off words.

---

## File Structure

```
simple-voice-interface/
├── main.py              # Entry point only. Wires components, calls asyncio.run().
├── state.py             # VoiceStateMachine — FSM, task ownership, event management
├── agent.py             # LLMAgent — Phase 1: streaming LLM. Phase 2: OpenClaw WS client.
├── stt.py               # StreamingSTT — mic audio in, transcript strings out
├── tts.py               # StreamingTTS — text tokens in, audio chunks out
├── audio.py             # AudioController — mic capture, VAD, playback, cancellation
├── config.py            # All constants and configuration in one place
├── requirements.txt
├── .env.example
├── .env                 # git-ignored
└── CLAUDE.md            # This file
```

### Strict File Responsibilities

**`main.py`** — Maximum 40 lines. Imports all modules, instantiates all classes,
passes dependencies via constructor injection, calls `asyncio.run(state_machine.run())`.
Contains zero business logic.

**`state.py`** — Contains `ConversationState` enum and `VoiceStateMachine` class.
`VoiceStateMachine` owns: `self.state`, `self.active_tasks`, `self.interrupt_event`,
`self.utterance_queue`, `self.token_queue`, `self.audio_queue`.
All state transitions happen here and nowhere else.

**`agent.py`** — Contains `LLMAgent` class. Single public method:
`async def run(self, transcript: str) -> AsyncIterator[str]`.
Phase 1: makes a streaming OpenAI-compatible API call, yields tokens as they arrive.
Phase 2: connects to OpenClaw Gateway WebSocket, yields response tokens.
The rest of the codebase never changes between phases.

**`stt.py`** — Contains `StreamingSTT` class. Receives raw audio frames from a queue,
streams them to the Deepgram WebSocket API, yields final transcript strings.
Handles interim results internally — never yields them upstream.

**`tts.py`** — Contains `StreamingTTS` class. Receives a text token async iterator,
streams tokens to the TTS provider as they arrive, yields audio chunk bytes.
Begins yielding audio before the full response text is complete.

**`audio.py`** — Contains `AudioController` class. Responsibilities:
  - Mic capture via `sounddevice` (callback-based, bridges to asyncio via call_soon_threadsafe)
  - VAD via `webrtcvad` — classifies 20ms frames as speech or silence
  - Silence duration tracking — 600ms continuous silence after speech triggers utterance_complete
  - Audio playback from `asyncio.Queue` of audio chunks
  - Playback cancellation: watches `interrupt_event`, flushes buffer on cancel
  - Does NOT make any AI or network calls

**`config.py`** — Contains a single `Config` dataclass. All constants live here.
Loaded from `.env` via `python-dotenv`. No hardcoded values anywhere else in the codebase.

---

## Component Interfaces

These are the contracts between components. Do not deviate from them.

### AudioController

```python
class AudioController:
    async def start_capture(self, audio_queue: asyncio.Queue) -> None:
        """Begin streaming 20ms PCM frames into audio_queue."""

    async def stop_capture(self) -> None:
        """Stop mic stream cleanly."""

    async def play(self, audio_queue: asyncio.Queue, interrupt_event: asyncio.Event) -> None:
        """Drain audio_queue to speaker.
        Return immediately when interrupt_event is set.
        Flush output buffer before returning."""

    def vad_is_speech(self, audio_frame: bytes) -> bool:
        """Return True if the 20ms frame contains speech. Stateless."""
```

### StreamingSTT

```python
class StreamingSTT:
    async def stream(self, audio_queue: asyncio.Queue, utterance_event: asyncio.Event) -> AsyncIterator[str]:
        """Consume audio frames from audio_queue.
        Yield final transcript strings (one per complete utterance).
        Set utterance_event when a final transcript is ready.
        Handle interim results internally."""
```

### LLMAgent

```python
class LLMAgent:
    async def run(self, transcript: str) -> AsyncIterator[str]:
        """Accept a transcript. Yield response text tokens as produced.
        Phase 1: streaming OpenAI-compatible API call.
        Phase 2: OpenClaw Gateway WebSocket client.
        Interface is identical in both phases."""
```

### StreamingTTS

```python
class StreamingTTS:
    async def synthesize(self, token_stream: AsyncIterator[str]) -> AsyncIterator[bytes]:
        """Consume text tokens. Yield audio chunk bytes.
        Begin yielding audio before token_stream is exhausted."""
```

### VoiceStateMachine

```python
class VoiceStateMachine:
    async def run(self) -> None:
        """Main loop. Start in LISTENING. Run until KeyboardInterrupt."""

    async def _transition_to(self, new_state: ConversationState) -> None:
        """Acquire transition_lock. Cancel and await all active_tasks.
        Set self.state. Start new state's coroutines. Release lock."""
```

---

## Concurrency Model

**Language:** Python 3.11+
**Concurrency:** `asyncio` exclusively. No threads. No multiprocessing.
**Exception:** `sounddevice` runs its audio callback in a C-level thread.
Bridge from that thread into asyncio using:
```python
loop.call_soon_threadsafe(queue.put_nowait, frame)
```
Never call `queue.put_nowait` directly from the sounddevice callback without this bridge.

### Queue Conventions

| Queue | Producer | Consumer | Contains |
|---|---|---|---|
| `mic_queue` | AudioController (sounddevice callback) | StreamingSTT | `bytes` — 20ms PCM frames at 16kHz |
| `token_queue` | LLMAgent | StreamingTTS | `str` — text tokens |
| `audio_queue` | StreamingTTS | AudioController playback | `bytes` — audio chunks |

All queues are `asyncio.Queue()` with no maxsize.

### Event Conventions

| Event | Set by | Cleared by | Meaning |
|---|---|---|---|
| `interrupt_event` | AudioController VAD (SPEAKING state only) | VoiceStateMachine on transition | User spoke during playback |
| `utterance_event` | StreamingSTT | VoiceStateMachine on transition | User finished speaking |

### Task Teardown Sequence

```python
# Signal all tasks to stop
for task in self.active_tasks:
    task.cancel()

# Wait for all tasks to fully exit
for task in self.active_tasks:
    try:
        await task
    except asyncio.CancelledError:
        pass  # Expected. Not an error.

# Clear before starting new tasks
self.active_tasks.clear()
```

This order is mandatory. Never start new tasks before active_tasks is cleared.

---

## Technology Choices

### Audio Capture + Playback
- **Library:** `sounddevice`
- **Format:** PCM 16-bit signed integer, mono
- **Sample rate:** 16000 Hz
- **Chunk size:** 20ms = 320 samples at 16kHz (required by webrtcvad)

### Voice Activity Detection
- **Library:** `webrtcvad`
- **Aggressiveness:** 2 (configurable in config.py, range 0–3)
- **End-of-utterance:** 600ms continuous silence after any speech (configurable)

### Speech-to-Text
- **Provider:** Deepgram streaming WebSocket API
- **Model:** `nova-2`
- **Mode:** True streaming — frames sent continuously, transcripts received in real time

### Text-to-Speech
- **Provider:** ElevenLabs streaming API (preferred) or Cartesia
- **Mode:** Streaming — text sent as tokens arrive, audio received before text is complete
- **Audio format:** PCM 16-bit or MP3. Convert to match sounddevice if needed.

### Agent — Phase 1
- **Implementation:** Streaming OpenAI-compatible LLM call with `stream=True`
- **Token iteration:** `async for chunk in stream: yield chunk.choices[0].delta.content`
- **History:** `self.conversation_history` list, max 20 turns rolling, system prompt preserved
- **Cancellation:** wrap stream in try/except asyncio.CancelledError, close connection on cancel

### Agent — Phase 2 (deferred)
- **Implementation:** `websockets` client to `ws://127.0.0.1:18789` (OpenClaw Gateway)
- **Prerequisite:** OpenClaw installed and running as a daemon
- **Interface:** Identical to Phase 1

### Configuration
- **Library:** `python-dotenv`
- **All secrets and tunables in `.env`**

---

## Phase 1 agent.py — Required .env Variables

```
LLM_API_BASE=https://api.openai.com/v1
LLM_API_KEY=your_key_here
LLM_MODEL=gpt-4o-mini
LLM_SYSTEM_PROMPT=You are a helpful voice assistant. Be concise. Your responses will be spoken aloud.
```

---

## Error Handling

- **Network errors:** Catch, log WARNING, transition back to LISTENING. Never crash the loop.
- **CancelledError:** Always catch in task bodies. Clean up resources. Allow the coroutine to exit.
- **LLMAgent errors:** Catch in agent.py. Yield "Sorry, I ran into an issue." so TTS can speak it.
- **Audio device errors:** Log ERROR and exit. Unrecoverable without user intervention.
- **Unhandled task exceptions:** Log full traceback at ERROR. Transition to LISTENING.

---

## Code Style

- **Type hints:** Required on every function signature.
- **Docstrings:** Required on all classes and public methods.
- **Comments:** Explain WHY, never WHAT.
- **Line length:** 100 characters maximum.
- **No global state.** Constructor injection only.
- **No blocking calls.** `time.sleep()` is banned. Use `await asyncio.sleep()`.
- **Logging:** `logging` module only. Never `print()`.
  - DEBUG: per-frame audio events
  - INFO: state transitions, transcripts, agent responses
  - WARNING: recoverable errors
  - ERROR: quality-degrading failures

---

## Build Order (do not skip steps, do not proceed without passing each test)

| Step | File | Pass Condition |
|---|---|---|
| 1 | `config.py` | `python config.py` prints loaded config without error |
| 2 | `audio.py` (capture only) | Speaking shows DEBUG logs of incoming frame bytes |
| 3 | `audio.py` (add VAD) | SPEECH/SILENCE transitions log correctly and reliably |
| 4 | `stt.py` | Speaking a sentence produces the correct transcript within 1 second |
| 5 | `agent.py` | Hardcoded transcript prints tokens one-by-one (not all at once) |
| 6 | `tts.py` | Hardcoded sentence plays as audio within 1 second |
| 7 | `state.py` | One full turn logs LISTENING → THINKING → SPEAKING → LISTENING |
| 8 | `main.py` | Speak a sentence, hear the agent respond end-to-end |
| 9 | Interrupts | Barge-in stops agent cleanly, 10/10 consecutive tests pass |
| 10 | Hardening | All "Done" criteria below satisfied |

---

## What "Done" Looks Like (Phase 1)

1. `python main.py` starts without errors
2. Speaking produces a transcript in INFO logs
3. First word of agent response is heard within ~1 second of transcript finalizing
4. Speaking while agent is talking stops it and returns to LISTENING within 200ms
5. Interrupt test passes 10 consecutive times — no ghost audio, no hanging tasks
6. `Ctrl+C` exits cleanly — no hanging tasks, no audio artifacts
7. Every file has type hints, docstrings, and WHY comments on non-obvious lines
8. `pip install -r requirements.txt` works in a clean Python 3.11 venv
9. `.env.example` documents every variable with description and example value

---

## Constraints Quick Reference

| Constraint | Value |
|---|---|
| Language | Python 3.11+ |
| Concurrency | asyncio only |
| Audio | sounddevice |
| VAD | webrtcvad, aggressiveness=2 |
| STT | Deepgram nova-2, streaming |
| TTS | ElevenLabs or Cartesia, streaming |
| Agent Phase 1 | Streaming OpenAI-compatible LLM |
| Agent Phase 2 | OpenClaw Gateway WebSocket (deferred) |
| Frontend | None |
| Server | None |
| Global state | Prohibited |
| Blocking I/O | Prohibited |
| Hardcoded values | Prohibited |
| max lines main.py | 40 |
| History window | 20 turns max |
| Silence-to-utterance | 600ms (configurable) |
| Time-to-first-audio | < 1 second target |