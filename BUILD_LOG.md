# BUILD_LOG.md — Simple Voice Interface

**Generated:** 2026-02-24
**Codebase snapshot:** commit `03a15e8` (fixing the race condition)
**Status:** Phase 2 active — OpenClaw Gateway WebSocket client

---

## 1. Project Purpose

Simple Voice Interface is a local, real-time, interruptible voice pipeline designed to put a spoken conversation layer on top of an autonomous AI agent. The core problem it solves is that existing voice assistants either use blocking architectures — where the user hears nothing until the entire AI response has been generated — or they rely on hosted infrastructure like OpenAI's Realtime API, which trades latency for vendor lock-in and removes developer control over the pipeline. This project builds every stage of the pipeline explicitly: microphone capture feeds a live speech-to-text stream, the transcript goes to an autonomous agent (initially a streaming LLM call, now the OpenClaw Gateway), tokens from the agent feed a streaming text-to-speech engine as they arrive, and audio chunks land in a playback queue that the speaker drains in real time. The entire path from the user finishing a sentence to the first audible word of the response takes roughly 600ms to 1.5 seconds on a fast model — and crucially, the user can interrupt the agent at any point mid-sentence and the entire pipeline stops within 200ms, not just the speaker. The project exists as a portfolio-quality implementation of a voice pipeline with a full finite-state machine, genuine task cancellation, and a clean layered architecture that can be studied, understood, and built upon.

---

## 2. Architecture Overview

The system is a single Python asyncio event loop running five concurrent coroutines, coordinated by a three-state finite state machine. There is no web server, no browser, no REST API — just a local process, a microphone, and a speaker.

**The pipeline, stage by stage:**

Raw PCM audio is captured from the microphone by `AudioController` using the `sounddevice` library. The audio device callback runs in a dedicated PortAudio C thread outside the asyncio event loop; each 20-millisecond frame is bridged into the event loop using `loop.call_soon_threadsafe(queue.put_nowait, frame)` and placed in `mic_queue`. This is the only point in the codebase where a non-asyncio thread touches asyncio data structures, and the bridge makes it safe.

`StreamingSTT` consumes frames from `mic_queue` and streams them over a persistent WebSocket to the Deepgram `nova-2` model. Deepgram runs continuous speech recognition and fires events as it processes audio. The STT layer filters out interim partial results and only surfaces final, corrected transcripts. When a transcript is ready, it is placed on `transcript_queue` and `utterance_event` is set to notify the state machine.

`VoiceStateMachine` detects `utterance_event` in its polling loop and transitions from LISTENING to THINKING. It passes the transcript to `LLMAgent`, which opens a WebSocket connection to the OpenClaw Gateway, performs an Ed25519 device-authentication handshake, sends the transcript as an agent RPC request, and begins receiving streaming response events. Each incremental text token is placed on `token_queue`. When the first token appears in the queue, the state machine transitions to SPEAKING.

`StreamingTTS` consumes tokens from `token_queue` via an internal adapter and buffers them to phrase-sized chunks before sending each chunk to the ElevenLabs streaming API. ElevenLabs begins returning raw PCM audio before the full phrase is even sent — this streaming mode is what allows the first audio to begin within 200–400ms of the first flush. PCM chunks are placed on `audio_queue` as they arrive.

`AudioController.play()` drains `audio_queue` to the speaker via a `sounddevice.OutputStream`. Each chunk is a raw int16 PCM buffer that sounddevice can accept directly — no conversion, no codec, no intermediate file. While playback is running, a separate VAD watcher coroutine monitors `mic_queue` for sustained speech that would indicate the user wants to barge in.

**The state machine governs everything.** `VoiceStateMachine` in `state.py` is the only object that creates tasks and the only object that cancels them. No component calls another component directly. All inter-component communication flows through three `asyncio.Queue` instances (`mic_queue`, `token_queue`, `audio_queue`) and two `asyncio.Event` instances (`utterance_event`, `interrupt_event`). This design means the states and their transitions are visible in one place: if something goes wrong with the pipeline, the answer is always in `state.py`.

The three states are **LISTENING** (mic capture + STT active, waiting for the user to finish speaking), **THINKING** (agent generating a response, mic paused, waiting for first token), and **SPEAKING** (TTS + playback + interrupt watcher active, waiting for either a barge-in or natural completion). Every state transition goes through `_transition_to()`, which acquires a `transition_lock`, cancels all active tasks, awaits them to full exit, drains relevant queues, commits the new state, and starts the new state's coroutines. The lock prevents two concurrent transitions from interleaving — a race condition that the Vocalis open-source project, which this project explicitly studied and improved upon, is vulnerable to.

---

## 3. File-by-File Breakdown

### `main.py` (41 lines)

`main.py` is the entry point and nothing more. It loads config, configures the logging system, instantiates one instance of each of the five components, passes all dependencies via constructor injection into `VoiceStateMachine`, and calls `asyncio.run(machine.run())`. There is zero business logic here. The 40-line ceiling is enforced by design — if main.py grows, it means logic has leaked from a component file into the wiring layer.

The `KeyboardInterrupt` handler at the bottom (`except KeyboardInterrupt: print("Goodbye.")`) is the only user-visible output in the file. Every other message goes through the logging system.

### `state.py` (468 lines)

`state.py` is the most important file in the codebase. It defines `ConversationState` (a three-value `Enum`) and `VoiceStateMachine`, which owns every asyncio primitive in the system: `mic_queue`, `token_queue`, `audio_queue`, `transcript_queue`, `utterance_event`, `interrupt_event`, `tts_done_event`, `transition_lock`, and `active_tasks`. It also tracks `_agent_task` separately from `active_tasks` for reasons explained in the design decisions section.

The public interface is a single method: `run()`. This enters the main polling loop, which sleeps 50ms per iteration and checks the current state. In LISTENING, it watches for `utterance_event`. In THINKING, it checks `not token_queue.empty()`. In SPEAKING, it watches for either `interrupt_event` or the combination of `audio_queue.empty() AND tts_done_event.is_set()`.

`_transition_to()` is the only place state changes happen. It is protected by `transition_lock`, performs the cancel-await-clear-start teardown sequence, and includes inline comments explaining every non-obvious queue-draining decision.

Four internal coroutines run as tasks owned by the machine: `_run_stt()` wraps the STT generator and forwards transcripts; `_run_agent()` iterates the agent stream and forwards tokens, sending a `None` sentinel at the end (even on cancellation); `_run_tts()` bridges `token_queue` into the TTS synthesizer and sets `tts_done_event`; and `_watch_for_interrupt()` polls `mic_queue` during SPEAKING, classifying frames with the VAD and setting `interrupt_event` after 5 consecutive speech frames.

What `state.py` does NOT do: it does not make any network calls, does not process audio frames directly, does not know the format of audio data, and does not implement any component logic. It is purely an orchestrator.

### `agent.py` (341 lines)

`agent.py` implements `LLMAgent`, the Phase 2 OpenClaw Gateway WebSocket client. The single public method is `async def run(self, transcript: str) -> AsyncIterator[str]`, an async generator that yields response text tokens.

Each call to `run()` opens a fresh WebSocket connection to `ws://127.0.0.1:18789` and performs the full four-step handshake: receive the challenge (a JSON nonce from the Gateway), send a signed connect request (with an Ed25519 device signature built by `_build_device_section()`), receive the `hello-ok` confirmation, then send the agent RPC request with the transcript and `agentId: "main"`. After that, the generator loops over incoming WebSocket messages, ignoring everything except `type=="event" AND event=="agent"` frames, and yielding the `payload.data.delta` field from `stream=="assistant"` events until a `stream=="lifecycle" AND phase=="end"` event signals completion.

Two timeout layers protect the streaming loop: a 15-second timeout waits for the first token (covering LLM thinking time), and a 30-second timeout applies to subsequent tokens (covering slow streaming mid-response). On either timeout, a spoken error token is yielded so TTS always has something to play.

Three top-level functions handle device key management: `_get_device_key()` loads an existing Ed25519 key from `~/.openclaw/voice-device-key.pem` or generates and persists a new one; `_build_device_section()` constructs the pipe-delimited signing payload (`v2|{deviceId}|{clientId}|...`) and signs it; `_base64url_encode()` produces unpadded base64url encoding as required by the protocol.

What `agent.py` does NOT do: it does not manage conversation history (each turn is a fresh request), does not implement the Phase 1 LLM path (that code was replaced in Phase 2), and does not interact with any other component besides the Gateway WebSocket.

### `stt.py` (369 lines)

`stt.py` implements `StreamingSTT`, which wraps Deepgram's WebSocket streaming API behind an async generator interface. The key design choice is the persistent connection: `connect()` opens the Deepgram WebSocket once at startup and keeps it alive for the entire session through `_keep_connection_alive()`, a long-lived task that holds the `async with` block open, registers event handlers, and retries with exponential backoff (capped at 8 seconds) on connection drops.

The `stream()` method, called on each LISTENING entry, does not open a new WebSocket — it attaches a `_sender` task to the already-open connection. The sender task reads frames from `mic_queue` and calls `connection.send_media(frame)`. A separate keepalive task fires every 8 seconds, sending `{"type": "KeepAlive"}` to prevent Deepgram's 10-second idle timeout from closing the connection during THINKING and SPEAKING states when no audio is being sent.

Deepgram events are handled by `_on_persistent_message()`, which is registered once on the persistent connection and writes only `is_final` transcripts (non-empty) to an internal `_transcript_queue`. `stream()` reads from this queue with a 5-second polling timeout, checking whether the listener task has exited on each timeout to detect silently dropped connections.

What `stt.py` does NOT do: it does not produce interim transcripts upstream (they are filtered in `_on_persistent_message`), does not implement any silence-duration logic, and does not interact with any component other than `mic_queue` and `utterance_event`.

### `tts.py` (209 lines)

`tts.py` implements `StreamingTTS`, which converts a streaming text token iterator into a streaming PCM audio iterator using the ElevenLabs `eleven_turbo_v2_5` model.

The `synthesize()` method accumulates incoming tokens in a buffer and flushes to ElevenLabs when either a punctuation character (`.!?,;:`) appears at the end of the buffered text or the buffer has accumulated at least 5 tokens without punctuation. This phrase-boundary flushing is critical for speech quality: sending single tokens to ElevenLabs resets its prosody model on every request, producing flat robotic output. Longer phrases give ElevenLabs enough context to produce natural intonation.

The `_stream_phrase()` helper sends one buffered phrase to ElevenLabs and yields PCM chunks. It applies a `tts_timeout`-second timeout only to the first chunk from ElevenLabs (covering the HTTP connection and synthesis start), then yields subsequent chunks without timeout. The output format is `pcm_16000` — raw int16 mono PCM at 16 kHz — which matches the format sounddevice expects, eliminating any codec conversion step.

What `tts.py` does NOT do: it does not know anything about the state machine, does not read from `token_queue` directly (it receives a `token_stream: AsyncIterator[str]` argument), and does not handle audio playback.

### `audio.py` (309 lines)

`audio.py` implements `AudioController`, the only component that touches hardware. It has three responsibilities: microphone capture, VAD classification, and audio playback.

`start_capture()` opens a `sounddevice.InputStream` in callback mode. The callback runs in a PortAudio C thread and uses `loop.call_soon_threadsafe(queue.put_nowait, frame)` to safely bridge into asyncio. The coroutine suspends on an internal `asyncio.Event` until either `stop_capture()` sets it or the task is cancelled; in both cases, the `finally` block closes the sounddevice stream to prevent double-producer bugs.

`vad_is_speech()` applies a two-layer filter: first an RMS energy check (rejecting frames below `vad_energy_threshold = 300.0` RMS units), then `webrtcvad.Vad.is_speech()`. The energy gate exists because webrtcvad classifies waveform shape, not amplitude — it will label quiet hiss as speech if the pattern looks right. The RMS check rejects near-silence before the VAD even runs.

`play()` opens a `sounddevice.OutputStream` and drains `audio_queue` to the speaker in a loop. Each chunk is decoded from bytes to a numpy int16 array and written via `await loop.run_in_executor(None, stream.write, audio_array)`. The `run_in_executor` call offloads the blocking `stream.write()` call to a thread pool, keeping the asyncio event loop free to service interrupt checks and other coroutines during the write. The loop checks `interrupt_event.is_set()` at each iteration; when the event is set, the loop exits and the `with` block's `__exit__` calls `stream.stop()`, which flushes the PortAudio output buffer.

What `audio.py` does NOT do: it does not track silence duration, does not fire utterance events, does not interact with Deepgram or ElevenLabs, and does not import or reference the state machine.

### `config.py` (166 lines)

`config.py` defines the `Config` frozen dataclass, which is the single source of truth for every constant and tunable in the codebase. Fields with defaults are optional; fields without defaults (`deepgram_api_key`, `tts_api_key`, `tts_voice_id`) are required and will cause `load()` to raise `ValueError` with a clear message if absent. `chunk_size` is the one derived field — computed as `sample_rate * chunk_ms // 1000` inside `load()` — because webrtcvad's frame-size requirement makes it a pure function of the other two audio parameters.

The `load()` function reads `.env` via `python-dotenv`, then reads each variable from `os.environ`. It references field defaults via `dataclasses.fields()` so the default values are never duplicated between `Config` and `load()`.

### `openclaw_stub.js`

A Node.js WebSocket server that simulates the OpenClaw Gateway for testing. It accepts connections at `ws://127.0.0.1:18789`, expects `{ type: "transcript" }` messages (the pre-Phase-2 protocol), and streams back canned responses token-by-token with 60ms inter-token delay. The three canned responses are long enough (3–4 sentences) to provide a comfortable window for testing the interrupt path. The stub is intentionally disconnected from the real Gateway protocol: the real `agent.py` speaks the full four-step handshake protocol, while the stub speaks a simpler custom protocol designed for early pipeline testing. To use the stub with the current `agent.py`, the stub would need to be updated to speak the correct challenge/connect/hello-ok/event protocol. Its primary value now is as documentation of the pre-Phase-2 protocol design.

### `.env.example`

Documents every configuration variable the system reads, with descriptions, example values, and guidance on typical ranges for tunable parameters. Every variable has a `REQUIRED` or `OPTIONAL` annotation. The file is the first thing a new developer reads after cloning the repository.

---

## 4. Key Design Decisions

**asyncio-only concurrency.** The entire application runs in a single asyncio event loop with no threads (except the unavoidable PortAudio callback thread managed by sounddevice). This eliminates a class of concurrency bugs that arise from shared mutable state across threads. The `call_soon_threadsafe` bridge is the single, explicit crossing point between the C thread and asyncio — narrow and easy to audit.

**Cancel-before-start teardown order.** Every state transition cancels all active tasks and awaits them to full exit before creating new tasks. The alternative — starting new tasks first, cancelling old ones after — creates a window where two mic_capture tasks exist simultaneously, both writing to `mic_queue`. The resulting doubled audio frames would corrupt both STT and VAD. The mandatory teardown order closes this window.

**`transition_lock` prevents concurrent transitions.** The lock ensures that if an interrupt fires while a THINKING→SPEAKING transition is already in progress, the interrupt's SPEAKING→LISTENING transition blocks until the first transition completes. Without the lock, the two transitions could interleave, creating orphaned tasks or inconsistent `active_tasks` state. This is the specific race condition that the Vocalis `handle_audio()` function is vulnerable to: it can be called concurrently, with the second call overwriting `self.current_audio_task` before the first task is awaited.

**Persistent STT connection, per-turn agent connection.** The Deepgram WebSocket is opened once at startup and held open for the session lifetime. This eliminates the 300–500ms TLS + HTTP upgrade handshake that would otherwise occur on every SPEAKING→LISTENING transition — the dominant controllable latency factor after Phase 1. The OpenClaw Gateway connection, by contrast, is opened fresh per turn. The reasoning is that the loopback handshake is negligible (~20ms total for challenge + connect + hello-ok), and a persistent agent connection would require keepalive complexity on the agent side while providing no meaningful latency benefit: the LLM TTFT (200ms–3s) already dominates the THINKING phase by orders of magnitude.

**`_agent_task` tracked separately from `active_tasks`.** During the THINKING state, `_agent_task` is not in `active_tasks`. When the THINKING→SPEAKING transition fires, `_transition_to()` cancels and awaits everything in `active_tasks` — but the agent task must not be cancelled here, because it is the producer feeding `token_queue` that TTS will consume in SPEAKING. Keeping it out of `active_tasks` during THINKING means the teardown sequence leaves it running. `_start_speaking()` then adds it to `active_tasks` so the subsequent SPEAKING→LISTENING teardown cancels it. If the agent finishes naturally before SPEAKING teardown, `task.cancel()` on an already-done task is a no-op.

**`None` sentinel in `token_queue`.** When `_run_agent()` exits — whether by natural completion or cancellation — its `finally` block unconditionally sends `None` to `token_queue`. The `_token_source()` generator inside `_run_tts()` reads from the queue and breaks on `None`. This decoupled shutdown signal ensures `_run_tts()` never blocks indefinitely on `token_queue.get()` waiting for a token that will never arrive, regardless of how the agent exits.

**`token_queue` not drained on SPEAKING entry.** When the THINKING→SPEAKING transition fires, the agent has already enqueued the token that triggered the transition — and possibly several more. Draining the queue here would silently discard the first words of the response. The general teardown logic drains `token_queue` on all other transitions (where stale tokens from a previous turn could corrupt the next turn's TTS input).

**`mic_queue` not drained on LISTENING entry.** When an interrupt fires, the user is actively speaking. The 5 frames of sustained speech that triggered the interrupt are still in `mic_queue`. Draining the queue on LISTENING entry would discard those frames — the user's leading syllables — and force them to repeat before Deepgram picks up the new utterance. Preserving them means the new STT sender immediately begins processing real speech.

**`transcript_queue` drained on LISTENING entry.** Deepgram's 60ms endpointing setting is aggressive enough that a natural mid-sentence pause splits one utterance into two `is_final` events. Both are enqueued on `transcript_queue`. The first triggers `utterance_event` and is consumed by the main loop; the second sits in the queue. Without draining on the next LISTENING entry, `transcript_queue.get()` on the following turn returns the stale second fragment rather than the user's new input. The drain at `state.py:260` closes this correctness bug.

**50ms polling loop.** The main loop in `run()` uses `await asyncio.sleep(0.05)` rather than `asyncio.wait()` with per-event conditions. The polling approach is vastly simpler to reason about — there is no re-arming logic, no partial-wakeup management, no cancellation of the wait set on each transition. The cost is a maximum 50ms latency on each condition detection, which is imperceptible in a voice conversation (50ms is roughly 1/20th of a spoken word). The two 50ms cycles in the LISTENING→THINKING→SPEAKING path add at most 100ms to the perceived silence.

**RMS energy gate before webrtcvad.** webrtcvad is a waveform-shape classifier: it evaluates whether the signal looks like speech, not whether it is loud enough to be speech. Low-amplitude hiss with the right spectral shape will fool it. The `_rms()` gate rejects frames below 300.0 RMS units before they ever reach webrtcvad, eliminating a class of false-positive interrupts from background noise.

**5 consecutive speech frames for interrupt detection.** The VAD interrupt watcher requires 5 consecutive speech frames (100ms of sustained speech) before setting `interrupt_event`. A single loud noise or brief mic bleed would fire the original single-frame check. Requiring a continuous run means the trigger is much more likely to be genuine speech. Any gap resets the counter, so the user must speak continuously for 100ms, not merely accumulate 5 scattered frames.

**2.0-second initial delay in the VAD watcher.** Speaker audio bleeds into the microphone during the first few hundred milliseconds of playback, especially at higher volumes. The 2-second delay gives the speaker time to stabilise and ensures the user has actually heard something before a barge-in is possible. The trade-off is that barge-ins are impossible in the first 2 seconds of any response — a known limitation.

**Token buffering in TTS with phrase-boundary flushing.** Sending individual tokens to ElevenLabs resets its prosody model on every request, producing flat, robotic speech where each word is synthesized in isolation. Buffering to punctuation boundaries or a minimum of 5 tokens gives ElevenLabs a phrase-sized chunk with enough context to model intonation naturally. The result is speech that sounds like a sentence, not a sequence of words.

**PCM output format for TTS.** ElevenLabs defaults to MP3 output. The codebase requests `output_format='pcm_16000'` — raw int16 mono PCM at 16 kHz. This matches the format sounddevice expects natively, eliminating the pydub/ffmpeg codec conversion step that would otherwise be required. The tradeoff is larger network payloads, which are irrelevant for a local pipeline.

---

## 5. Component Deep Dives

### VAD (Voice Activity Detection)

webrtcvad is a C extension built on Google's WebRTC noise suppression stack. It was originally designed for browser-based voice communication and is the same algorithm that Chrome uses for echo cancellation. In this application it runs in `vad_is_speech()` at `audio.py:50`, classifying 20ms frames of PCM audio as speech or silence.

The implementation uses aggressiveness level 2 (range 0–3). Level 0 is most permissive (accepts quieter, more ambiguous signals as speech); level 3 is most aggressive (only classifies strong, clear speech). Level 2 is the balance point used in most production voice applications: sensitive enough to catch normal speech in a quiet room, resistant enough to reject most ambient noise.

webrtcvad's critical constraint is the exact frame duration: it accepts only 10ms, 20ms, or 30ms frames at the configured sample rate. The codebase uses 20ms frames (320 samples at 16kHz), giving 50 frames per second. This is why `chunk_size = sample_rate * chunk_ms // 1000 = 16000 * 20 // 1000 = 320` is a derived value in config rather than a constant — changing `chunk_ms` to 30 automatically adjusts it.

The failure mode is false positives from background noise: typing, ambient HVAC, street noise. The RMS gate at `audio.py:77` filters these by rejecting frames below 300.0 RMS units without calling webrtcvad. The 300.0 threshold was chosen to reject quiet ambient noise while accepting normal speaking volume; the `.env.example` documents the typical ranges (100–300 for whisper environments, 800–2000 for loud environments only).

During SPEAKING, the VAD watcher (`_watch_for_interrupt`) is the only consumer of `mic_queue`. During LISTENING, the STT sender is the consumer. During THINKING, no task consumes from `mic_queue` — the queue accumulates frames from the capture task. This is intentional: VAD is paused during THINKING to prevent background noise from triggering a false interrupt before the agent has produced any audio.

### STT (Speech-to-Text)

Deepgram's `nova-2` model runs over a persistent WebSocket. The choice of true streaming over the alternatives (local Whisper, batch transcription) was deliberate: local Whisper (`faster-whisper`) would require buffering a complete utterance before transcribing, adding 300–2000ms of latency. Batch cloud transcription has similar buffering requirements. Deepgram's streaming mode sends frames continuously and returns transcripts as they arrive, making sub-200ms post-utterance transcription realistic.

The persistent connection architecture is the most important implementation detail in `stt.py`. The Deepgram SDK (`AsyncDeepgramClient` from `deepgram-sdk==5.3.2`, a FERN-generated SDK) opens a WebSocket with TLS handshake and HTTP upgrade. This takes 300–500ms on a cold connection. Phase 1 opened this connection per SPEAKING→LISTENING transition; every post-interrupt response began with that 400ms delay. Phase 2 opens it once in `run()` before the first `_transition_to(LISTENING)` and keeps it warm for the session.

The keepalive task fires every 8 seconds, sending `{"type": "KeepAlive"}` to prevent Deepgram's 10-second idle timeout. This fires during THINKING and SPEAKING when the sender task is not active. The 8-second interval provides a 2-second margin.

Failure modes: if the connection drops mid-utterance, `stream()` detects it via the 5-second polling timeout + `_listener_task.done()` check, sets `utterance_event`, and yields an empty string. The FSM receives the empty transcript, sends it to the agent, and the agent responds to nothing. The `_keep_connection_alive()` keeper retries with exponential backoff (1s, 2s, 4s, 8s cap) and re-establishes the connection for the next turn.

### TTS (Text-to-Speech)

ElevenLabs `eleven_turbo_v2_5` is a low-latency streaming TTS model. The `AsyncElevenLabs` client (from `elevenlabs==2.36.1`, also FERN-generated) returns audio via `text_to_speech.stream()`, which is an async iterator rather than a coroutine — it must be iterated with `async for`, not awaited.

The phrase-boundary buffering in `synthesize()` is tuned for two competing goals: low latency (flush early so audio starts quickly) and speech quality (buffer enough text for natural prosody). The current settings — flush on punctuation or after 5 tokens — represent a reasonable balance. Flushing on comma allows phrases like "Well, first" to be synthesized as a unit rather than splitting at the comma. The 5-token minimum catches long unpunctuated runs from the agent.

Phrase synthesis is sequential: `synthesize()` uses `async for chunk in self._stream_phrase(joined): yield chunk` — one ElevenLabs HTTP call must complete before the next begins. This creates structural buffer underrun risk for multi-sentence responses: if phrase 1 finishes playing before phrase 2's ElevenLabs response arrives, `audio_queue` empties and the speaker goes silent mid-response. Concurrent phrase synthesis (issuing the next ElevenLabs call while the previous one is still being played) would eliminate this but requires prefetching logic and careful queue management.

The first-chunk timeout (`tts_timeout = 5.0` seconds) catches ElevenLabs outages and rate limits. Subsequent chunks have no timeout, which is safe because ElevenLabs delivers them rapidly once streaming starts. If the first-chunk timeout fires, `_stream_phrase()` returns without yielding, and `synthesize()` silently moves to the next phrase — or, if all phrases timeout, `audio_queue` stays empty and the FSM transitions naturally to LISTENING, leaving the user with silence and no error message.

### Agent (LLMAgent)

The agent is an async generator that hides the complexity of the OpenClaw Gateway protocol behind a single method: `run(transcript) -> AsyncIterator[str]`. The rest of the pipeline is entirely decoupled from the Gateway: `state.py` calls `self._agent.run(transcript)`, iterates the tokens, and puts them in `token_queue`. If the agent is swapped out (for a future direct LLM call, or a different gateway), `state.py` is untouched.

The Ed25519 device authentication is the most complex part of the implementation. The Gateway uses a challenge-response protocol where it sends a nonce, the client signs a pipe-delimited payload (`v2|{deviceId}|{clientId}|{mode}|{role}|{scopes}|{signedAtMs}|{authToken}|{nonce}`) using an Ed25519 private key, and includes the public key in the connect request. The device ID is the SHA-256 hex of the raw 32-byte public key, which ensures it is stable as long as the key file is stable. The key is stored at `~/.openclaw/voice-device-key.pem` in PKCS8 PEM format and reloaded on every process start.

For local (loopback) connections, the Gateway auto-approves the pairing, so no manual approval step is needed. The `client.id` and `client.mode` must be values from the Gateway's enum (`"cli"` for both), and `agentId: "main"` is required in the agent request — omitting it returns a `"Pass --to, --session-id, or --agent"` error from the Gateway.

The two-phase streaming timeout (15s for first token, 30s between subsequent tokens) was added after the diagnostic audit identified a critical issue: if the Gateway stalled without producing any assistant events, `token_queue` stayed empty indefinitely and the FSM was pinned in THINKING with no escape path.

Cancellation is handled by catching `asyncio.CancelledError` at the outermost `try` in `run()`. Because `run()` is inside `async with websockets.connect(...) as ws:`, the context manager's `__aexit__` sends a WebSocket close frame when the `async with` block exits — whether by normal return, by the `CancelledError` path, or by exception. No orphaned connections are left open.

### FSM (VoiceStateMachine)

The state machine is the central nervous system of the application. Its design philosophy is that every piece of system state belongs here, and every state change is explicit, logged, and serialized behind a lock.

The polling architecture (50ms sleep loop) was chosen over an event-driven approach (`asyncio.wait()` over multiple conditions) because polling is dramatically simpler to reason about. With `asyncio.wait()`, each condition would need to be wrapped as an `asyncio.Event`, the wait call would need to be re-armed after each wakeup, partial completions (only some conditions ready) would require special handling, and cancellation would need to cancel the wait itself. The polling loop has none of these concerns: it checks conditions once per iteration and takes action if they are met.

The teardown sequence in `_transition_to()` has four steps: signal (cancel), await, clear, start. The signal and await steps are separated so all tasks receive the cancel signal before any are awaited — this prevents a scenario where awaiting one task blocks while another task (which should also be cancelled) continues running and modifying shared state.

The `tts_done_event` is set by `_run_tts()` after the last audio chunk is placed in `audio_queue` — not by `_run_agent()` when the last token is produced. This is a subtle but important distinction: ElevenLabs may still be synthesizing when the agent finishes producing tokens. Setting the event in `_run_agent()` caused a 55ms premature SPEAKING→LISTENING transition in early testing, where `audio_queue.empty()` was coincidentally true (ElevenLabs hadn't started yet) and `tts_done_event` was already set.

### Audio (AudioController)

The audio layer is the only component that makes blocking calls. Two of these are notable:

The PortAudio callback is the only cross-thread call in the application. It runs at audio interrupt priority in a dedicated C thread managed by PortAudio, not by Python. `call_soon_threadsafe` is the correct way to schedule asyncio work from this thread — it enqueues the `put_nowait` call to run on the event loop thread, which is where every other asyncio operation runs.

`stream.write()` in `play()` blocks the calling thread until PortAudio accepts the audio data into its output buffer (up to one buffer period, approximately 20ms at 16kHz/320-sample blocks). `run_in_executor` moves this blocking call to the default thread pool executor, freeing the event loop to process interrupt events and other coroutines during the write. Without this, every audio write would freeze the event loop for up to 20ms, making interrupt detection unreliable.

---

## 6. Interrupt System

The barge-in (interrupt) system is the most latency-critical and failure-prone feature in the pipeline. It must reliably stop a speaking agent, clean up all active tasks, and return to LISTENING — all within 200ms of the user beginning to speak.

**Detection path:**

The VAD interrupt watcher (`_watch_for_interrupt`) starts as a task in `_start_speaking()`. It first sleeps for 2.0 seconds unconditionally (see timing discussion below), then enters a tight loop consuming frames from `mic_queue`. Each frame passes through `AudioController.vad_is_speech()`, which applies the RMS gate and webrtcvad classifier. A counter tracks consecutive speech frames. When the counter reaches 5 (100ms of continuous speech), `interrupt_event.set()` is called and the loop breaks.

The main polling loop in `run()` checks `interrupt_event.is_set()` every 50ms during SPEAKING. When it detects the event, it clears it and calls `await self._transition_to(ConversationState.LISTENING)`.

**Cancellation sequence:**

Inside `_transition_to(LISTENING)` from SPEAKING, the following tasks are in `active_tasks`: `mic_capture_speaking`, `tts_task`, `playback_task`, `vad_task`, and `_agent_task` (added to `active_tasks` in `_start_speaking()`). All five receive `task.cancel()` simultaneously, then all five are awaited in order. Each task catches `asyncio.CancelledError` in its body, logs, and re-raises (or in the case of `_run_agent`, does not re-raise to exit cleanly). The await loop catches `asyncio.CancelledError` and proceeds to the next task.

**What gets stopped and in what order:**

Signal order: all five tasks simultaneously. Await order: sequential (mic_capture, tts, playback, vad, agent). The sequential await has no functional impact — each task exits on its first await point after receiving the cancellation signal, which is typically 0–5ms.

**What is preserved vs drained:**

On SPEAKING→LISTENING transition, `audio_queue` is drained (stale PCM bytes from the interrupted response must not continue playing), and `token_queue` is drained (stale tokens from the cancelled agent must not trigger a new THINKING→SPEAKING). `mic_queue` is explicitly NOT drained — the 5 frames of sustained speech that triggered the interrupt (plus any additional speech frames queued during the 0–50ms polling delay) are preserved and delivered to the new STT sender immediately. This means Deepgram begins receiving the user's new speech within 1–5ms of the new LISTENING entry, without requiring the user to repeat the leading syllables.

**Timing guarantees:**

The earliest possible interrupt detection after SPEAKING starts is 2.1 seconds: 2.0 second initial delay plus 5 × 20ms = 100ms for consecutive frame detection, plus 0–50ms polling delay, plus 10–30ms teardown. Total: approximately 2.15–2.18 seconds after SPEAKING entry for the pipeline to return to LISTENING.

The 2.0-second delay is a deliberate trade-off against speaker bleed. Speaker audio during the first ~300ms of playback creates a real acoustic path into the microphone, and the VAD would classify it as speech immediately without the delay. The 2.0-second window is conservative — 500ms would likely be sufficient for most speaker/microphone combinations, but 2.0s provides a large margin against the false-interrupt problem.

**Important edge case — queue backlog during the delay:**

During the 2-second VAD delay, `mic_capture_speaking` is running and pushing frames to `mic_queue`. The VAD watcher is sleeping and not consuming them. After 2 seconds, the queue holds approximately 100 frames. The watcher processes all backlogged frames immediately when it wakes. If the user spoke at t=1.5s (0.5 seconds into the 2-second delay), their speech frames are in the backlog and are processed at full speed when the watcher wakes at t=2.0s. This means the 2-second delay guarantees the detection is delayed by up to 2 seconds, but does not guarantee silence for 2 seconds — queued speech is processed immediately when the window opens.

---

## 7. Known Issues and Mitigations

The following issues were identified in the diagnostic audit (`last_diagnostic.md`). Status reflects the current codebase at commit `03a15e8`.

**CRITICAL — THINKING hangs indefinitely if OpenClaw stalls (agent.py:278–291)**
*Original state:* The streaming loop `async for raw in ws:` had no timeout. If the Gateway accepted the request but never produced assistant events, `token_queue` stayed empty forever, pinning the FSM in THINKING with no escape.
*Current state:* **FIXED.** A two-phase timeout was added: 15 seconds for the first token (`first_token_received=False` branch), 30 seconds between subsequent tokens. On either timeout, `timed_out=True` is set and a spoken error token is yielded after the loop, ensuring the pipeline always produces audio and advances to SPEAKING.

**CRITICAL — Stale transcript race (state.py:259–261)**
*Original state:* Deepgram's 60ms endpointing split natural-pause sentences into two `is_final` events. The second fragment sat in `transcript_queue` across turns. On the next LISTENING entry, `transcript_queue.get()` returned the stale fragment rather than the user's new utterance.
*Current state:* **FIXED.** `_transition_to()` now drains `transcript_queue` and clears `utterance_event` on every entry to LISTENING. The drain is at `state.py:259–261`.

**CRITICAL — Auth handshake has no timeout (agent.py:183, 221)**
*Original state:* `ws.recv()` awaiting the challenge and hello-ok messages had no timeout. A Gateway that accepted the connection but stalled would block indefinitely.
*Current state:* **FIXED.** Both `ws.recv()` calls are wrapped with `asyncio.wait_for(..., timeout=10.0)`. On timeout, `ConnectionError` is raised, caught by the outer `except Exception`, and the error utterance is spoken.

**HIGH — 2.0-second hard delay before barge-in (state.py:428)**
*Current state:* **Open.** The `await asyncio.sleep(2.0)` in `_watch_for_interrupt()` prevents any interrupt detection in the first 2 seconds of SPEAKING. This is a deliberate trade-off against speaker bleed causing false interrupts, but it means the user cannot barge in at the very start of the agent's response. An adaptive delay based on measured audio levels (start listening after speaker amplitude drops below a threshold) would improve this but has not been implemented.

**HIGH — ElevenLabs 429 silently produces no audio (tts.py:154–158)**
*Current state:* **Open.** When ElevenLabs returns an HTTP 4xx error (rate limit, invalid API key), the exception is caught in `synthesize()` by `except Exception as exc: logger.warning(...)` and the generator exits silently. No error utterance is spoken, no spoken feedback is given. The FSM detects `audio_queue.empty() AND tts_done_event.set()` and transitions back to LISTENING. The user hears silence and the system appears to stop responding.

**HIGH — No conversation history (agent.py:244–261)**
*Current state:* **Open.** Each call to `run()` sends only the current transcript to the Gateway with no session context. The agent request uses `agentId: "main"` without a `sessionId` or any history payload. Whether OpenClaw's "main" agent maintains its own persistent memory is opaque to the voice interface. A user who asks a follow-up question ("Can you explain that again?") may receive a response with no referent.

**HIGH — Buffer underrun: serial ElevenLabs calls (tts.py:145, audio.py:219)**
*Current state:* **Open.** `synthesize()` processes phrases sequentially: the next ElevenLabs HTTP call does not start until the previous one completes. For a multi-sentence response, if the audio from phrase 1 finishes playing before phrase 2's ElevenLabs response arrives, `audio_queue` empties and the speaker outputs silence mid-response. `tts_done_event` is not set until all phrases complete, so the FSM does not declare completion — but the user hears a gap. This is a structural limitation of the single-connection sequential phrase approach.

**MEDIUM — 60ms Deepgram endpointing splits natural-pause sentences (config.py:51)**
*Current state:* **Open.** `deepgram_endpointing_ms=60` is very aggressive. A natural thinking pause longer than 60ms (for example, "I want to... ask about the weather") causes Deepgram to finalize "I want to" as a complete utterance. The stale-transcript drain mitigates the correctness bug, but the split utterance problem remains: the agent receives an incomplete sentence. Increasing `DEEPGRAM_ENDPOINTING_MS` to 200–500ms in `.env` reduces splits at the cost of slightly higher post-utterance latency.

**MEDIUM — `silence_duration_ms` is a dead config value (config.py:46)**
*Current state:* **Open.** `Config.silence_duration_ms = 600` is set in the dataclass and loaded from `.env` but never read by any component. It is a remnant of a local VAD endpointing design that was superseded by Deepgram's own endpointing. Deepgram's `endpointing_ms` is the only parameter that controls when a transcript finalizes. The value should either be deleted from `Config` or wired into the STT layer to replace Deepgram's endpointing with a local silence counter.

**LOW — Two 50ms polling cycles in the critical latency path**
*Current state:* **Open.** The LISTENING→THINKING transition adds up to 50ms (one poll cycle) to detect `utterance_event`. The THINKING→SPEAKING transition adds up to another 50ms to detect `not token_queue.empty()`. In the worst case, these two cycles add 100ms to the perceived silence gap. Event-driven wakeups using `asyncio.Event.wait()` would reduce this to near zero but add significant complexity to the run loop.

**LOW — Deepgram drop falls back with 5-second wait then sends empty string (stt.py:296–302)**
*Current state:* **Open.** When the Deepgram connection drops mid-utterance, `stream()` waits up to 5 seconds on the queue timeout before detecting the drop and yielding an empty string. The FSM sends the empty transcript to the agent, whose behavior with an empty message is undefined. The 5-second wait is an unavoidable consequence of using polling to detect connection state. A better approach would be registering on the `EventType.CLOSE` event to immediately signal failure.

---

## 8. Configuration Reference

All configuration is loaded by `config.load()` from `.env` via `python-dotenv`. Values not in `.env` fall back to the defaults declared on the `Config` dataclass. Changing a value requires only editing `.env` — no code changes.

### Required (no default — must be in `.env`)

**`DEEPGRAM_API_KEY`** (`config.deepgram_api_key`)
Deepgram API key. Obtained from `https://console.deepgram.com`. A free tier is available. Without this, the application raises `ValueError` on startup.

**`TTS_API_KEY`** (`config.tts_api_key`)
API key for the TTS provider (ElevenLabs or Cartesia). Without this, `ValueError` on startup.

**`TTS_VOICE_ID`** (`config.tts_voice_id`)
Voice identifier for the TTS provider. For ElevenLabs, this is the voice library ID visible in the ElevenLabs dashboard (example: `21m00Tcm4TlvDq8ikWAM` for the "Rachel" voice).

### Audio

**`SAMPLE_RATE`** (`config.sample_rate`, default: `16000`)
Microphone sample rate in Hz. 16kHz is required by webrtcvad. Do not change unless a different STT provider requires it.

**`CHUNK_MS`** (`config.chunk_ms`, default: `20`)
Audio frame duration in milliseconds. webrtcvad accepts only 10, 20, or 30. `chunk_size` is automatically derived as `sample_rate * chunk_ms // 1000`. 20ms gives 50 frames per second — sufficient time resolution for voice activity detection without excessive processing overhead.

### VAD

**`VAD_AGGRESSIVENESS`** (`config.vad_aggressiveness`, default: `2`)
webrtcvad aggressiveness level (0–3). Higher values are more aggressive (require stronger, cleaner speech signal). 2 is the standard choice for indoor desktop microphones. Increase to 3 if background noise causes false interrupts. Decrease to 1 if quiet speech is being missed.

**`SILENCE_DURATION_MS`** (`config.silence_duration_ms`, default: `600`)
*Dead value — not read by any component.* Originally intended to control local silence-duration endpointing. Deepgram's `DEEPGRAM_ENDPOINTING_MS` is the operative parameter.

**`VAD_ENERGY_THRESHOLD`** (`config.vad_energy_threshold`, default: `300.0`)
RMS amplitude gate applied before webrtcvad. Frames with RMS below this value are classified as silence without calling the VAD. Typical ranges: 100–300 for quiet environments or whisper-level speech; 300–800 (default) for normal speech in a moderately quiet room; 800–2000 for loud environments or to ignore quiet background sounds.

### STT

**`DEEPGRAM_MODEL`** (`config.deepgram_model`, default: `"nova-2"`)
Deepgram model identifier. `nova-2` is Deepgram's current best accuracy/latency model for general English speech. Other options: `nova`, `enhanced`, `base`. Higher-accuracy models increase transcription latency.

**`DEEPGRAM_ENDPOINTING_MS`** (`config.deepgram_endpointing_ms`, default: `60`)
Milliseconds of silence Deepgram waits before finalizing a transcript. Lower values respond faster after you stop speaking; higher values reduce false cuts on natural mid-sentence pauses. 60ms is aggressive — natural thinking pauses longer than 60ms will split the utterance. The `.env.example` comment suggests 200ms as a more conservative default, but 60ms was chosen for low-latency response at the cost of occasional split utterances.

### TTS

**`TTS_PROVIDER`** (`config.tts_provider`, default: `"elevenlabs"`)
TTS provider name. Currently only `"elevenlabs"` is implemented. `"cartesia"` is mentioned as an alternative in the spec but not wired up.

**`TTS_TIMEOUT`** (`config.tts_timeout`, default: `5.0`)
Seconds to wait for the first audio chunk from ElevenLabs before giving up on a phrase. If ElevenLabs does not respond in this window, the phrase is silently skipped. 5 seconds is generous; typical ElevenLabs first-chunk latency is 200–400ms. Reduce to 2.0 for faster failure detection.

### LLM (Phase 1 — kept for Phase 1 compatibility)

**`LLM_API_BASE`** (`config.llm_api_base`, default: `"https://api.openai.com/v1"`)
Base URL for an OpenAI-compatible LLM API. Unused in Phase 2.

**`LLM_API_KEY`** (`config.llm_api_key`, default: `""`)
LLM provider API key. Unused in Phase 2.

**`LLM_MODEL`** (`config.llm_model`, default: `"gpt-4o-mini"`)
LLM model name. Unused in Phase 2.

**`LLM_SYSTEM_PROMPT`** (`config.llm_system_prompt`, default: `"You are a helpful voice assistant..."`)
System prompt sent at the start of every conversation. Unused in Phase 2 (the system prompt is owned by OpenClaw's agent configuration).

**`LLM_MAX_HISTORY_TURNS`** (`config.llm_max_history_turns`, default: `20`)
Rolling conversation history window. Unused in Phase 2.

### OpenClaw Gateway (Phase 2)

**`LLM_WS_URL`** (`config.llm_ws_url`, default: `"ws://127.0.0.1:18789"`)
WebSocket URL of the running OpenClaw Gateway. The loopback default assumes the daemon runs on the same machine.

**`OPENCLAW_GATEWAY_TOKEN`** (`config.openclaw_gateway_token`, default: `""`)
Auth token used in the Gateway connect request and included in the Ed25519 signing payload. Found in `~/.openclaw/openclaw.json` → `gateway.auth.token`.

**`OPENCLAW_CMD`** (`config.openclaw_cmd`, default: `"openclaw"`)
Binary name for the OpenClaw CLI. Stored as a config value for reference but not used by the WebSocket approach.

### Logging

**`LOG_LEVEL`** (`config.log_level`, default: `"INFO"`)
Python logging level: `DEBUG`, `INFO`, `WARNING`, `ERROR`. `DEBUG` logs every audio frame classification (50 events/second) and every STT frame send — very noisy but useful for diagnosing VAD and STT issues. `INFO` logs state transitions, transcripts, agent responses, and connection events. Use `INFO` for normal operation.

---

## 9. Latency Profile

The path from the moment the user stops speaking to the moment they hear the first audio word of the response spans multiple stages, each with its own contribution. The table below uses values from the diagnostic audit.

**Stage 1: Deepgram endpointing (60ms)**
After the user's last syllable, Deepgram requires 60ms of continuous silence before it emits a final transcript. This is the `deepgram_endpointing_ms` value. The user perceives this as the minimum gap between finishing a sentence and the system beginning to respond.

**Stage 2: Deepgram network RTT (50–150ms)**
The transcript travels from Deepgram's servers to the application. On the persistent connection, there is no TLS handshake overhead — only processing and network latency. Deepgram's nova-2 model typically adds 50–150ms here.

**Stage 3: Polling delay, utterance→THINKING (0–50ms)**
The main loop checks `utterance_event.is_set()` every 50ms. On average, this adds 25ms; worst case 50ms.

**Stage 4: FSM teardown, LISTENING→THINKING (5–20ms)**
Cancelling and awaiting `mic_capture` and `stt_task`, closing the PortAudio stream.

**Stage 5: WebSocket connect to OpenClaw (5–15ms)**
Opening a new TCP connection to the loopback address. No TLS (ws://, not wss://).

**Stage 6: Gateway auth handshake (5–20ms)**
Three WebSocket messages: challenge received, connect request sent, hello-ok received. All loopback.

**Stage 7: First token from OpenClaw (200ms–3s+)**
The dominant variable in the entire pipeline. This is the LLM time-to-first-token (TTFT): time from when the Gateway receives the request to when the underlying model produces its first output token. On GPT-4o via fast infrastructure, this is typically 200–500ms. On slower models or under load, it can exceed 3 seconds. This stage is entirely outside the voice interface's control.

**Stage 8: Polling delay, first token→SPEAKING (0–50ms)**
The main loop checks `not token_queue.empty()` every 50ms. Average 25ms, worst case 50ms.

**Stage 9: TTS token buffering (100–500ms at typical token rates)**
`synthesize()` does not call ElevenLabs until the buffer contains 5 tokens or a punctuation character. At 30 tokens/second (a typical GPT-4o-class model), 5 tokens take ~167ms to arrive. At 10 tokens/second, this is 500ms. Flushing on punctuation can trigger earlier; "Hello." is two tokens and flushes immediately on the period.

**Stage 10: ElevenLabs first-chunk latency (200–400ms)**
Time from when the ElevenLabs HTTP request is sent to when the first audio bytes arrive. This is the TTFB of the ElevenLabs streaming API for `eleven_turbo_v2_5`.

**Total perceived silence (fast path): ~630ms**
60 + 50 + 25 + 10 + 5 + 10 + 200 + 25 + 167 + 200 = ~752ms in the fast-path median case. On a fast LLM with a short first phrase, 630ms is achievable.

**Total perceived silence (slow path): 4.2s+**
With a 3s LLM TTFT, slow buffering, and ElevenLabs at high latency, the total easily exceeds 4 seconds.

**What can be improved:**
Stages 3, 4, 5, 6, 8 total about 80–155ms and could be reduced with event-driven wakeups instead of polling (stages 3, 8) and a persistent agent connection (stages 5, 6). Stage 9 (TTS buffering) could be reduced by flushing on 2–3 tokens rather than 5, at the cost of some prosody quality for the first phrase. Stage 10 (ElevenLabs TTFB) is largely fixed.

**What cannot be improved by this application:**
Stage 7 (LLM TTFT) is entirely determined by the underlying model and infrastructure. It is the dominant variable and completely outside the voice interface's control.

---

## 10. Testing and Validation

### Running the full pipeline

Ensure the following prerequisites are in place: a `.env` file with `DEEPGRAM_API_KEY`, `TTS_API_KEY`, and `TTS_VOICE_ID` populated; Python 3.11+ with `pip install -r requirements.txt` run in a virtual environment; either the OpenClaw daemon running (`openclaw gateway`) with `OPENCLAW_GATEWAY_TOKEN` set, or the stub server running for simplified testing.

Start the application:

```
python main.py
```

Expected startup sequence in the logs: `INFO Voice pipeline starting` → `INFO STT connection ready` → `INFO State: none → listening` → `INFO Listening...`. After this, speaking into the microphone should produce `INFO TRANSCRIPT: <your words>` followed by `INFO State: listening → thinking` → `INFO State: thinking → speaking` → `INFO Speaking...`.

### Running with the OpenClaw stub

Note: the stub (`openclaw_stub.js`) uses an older, simpler protocol (custom `type: transcript` messages) that does not match the current `agent.py` Phase 2 handshake. To use the stub, install Node.js and run:

```
npm install ws
node openclaw_stub.js
```

The stub is most useful as a reference for what the pre-Phase-2 agent protocol looked like. To test with the stub, `agent.py` would need to be reverted to the Phase 1 WebSocket client design, or the stub would need to be updated to implement the full four-step Gateway protocol.

### Running with the real OpenClaw Gateway

Ensure the OpenClaw daemon is running (`openclaw gateway`), then copy `OPENCLAW_GATEWAY_TOKEN` from `~/.openclaw/openclaw.json` into `.env`. Set `LLM_WS_URL=ws://127.0.0.1:18789` (the default). `python main.py` will connect to the real Gateway and route requests to OpenClaw's configured default agent.

### Component-level validation

Each component file contains an `if __name__ == "__main__":` test harness. These are the Step 1–8 validation checkpoints from the CLAUDE.md build order:

- `python config.py` — Prints the fully resolved `Config` dataclass. Confirms API key loading and all default values.
- `python audio.py` — Captures 5 seconds of audio and logs `SPEECH`/`SILENCE` transitions per frame. Confirm that speaking shows `SPEECH started` and stopping shows `SILENCE started`.
- `python stt.py` — Runs the full STT pipeline for 10 seconds. Speaking a sentence should produce `INFO TRANSCRIPT: <your words>` within 1 second of finishing.
- `python agent.py` — Connects to the Gateway, sends "What is the speed of light? One sentence.", and prints tokens one by one. Tokens should arrive incrementally, not all at once.
- `python tts.py` — Synthesizes a hardcoded sentence and plays it. Confirms ElevenLabs connectivity, voice ID validity, and PCM playback without codec conversion.

### Interrupt path validation

To validate the interrupt system: start `python main.py`, speak a question, wait for the agent to begin speaking, then speak again while the agent is talking. Observe: `INFO VAD INTERRUPT TRIGGERED after 5 consecutive speech frames` in the logs, followed by `INFO State: speaking → listening`. Repeat 10 consecutive times. All 10 should interrupt cleanly with no ghost audio, no hanging tasks, and no `Task was destroyed but it is pending!` warnings.

Note: barge-ins are only possible after 2.0 seconds of SPEAKING. Speaking within the first 2 seconds will not trigger an interrupt (frames are queued but the VAD watcher is still sleeping).

### Diagnosing common failures

If no transcript appears after speaking: set `LOG_LEVEL=DEBUG` in `.env` and rerun. Confirm that `STT: sending frame 640 bytes` logs appear. If they do, the microphone is working; the issue is Deepgram (check API key, network). If they don't, the microphone is not capturing: check sounddevice device selection and permissions.

If agent never responds: confirm the Gateway is running and `OPENCLAW_GATEWAY_TOKEN` is correct. Run `python agent.py` standalone to test the Gateway connection in isolation.

If audio plays but sounds robotic or choppy: increase the TTS buffer by raising `_MIN_BUFFER_TOKENS` in `tts.py` from 5 to 8–10 tokens. Consider raising `DEEPGRAM_ENDPOINTING_MS` to reduce split utterances that produce very short first phrases.

---

## 11. Glossary

**asyncio.** Python's built-in asynchronous I/O framework. Uses cooperative multitasking: coroutines explicitly yield control at `await` points, and the event loop schedules the next ready coroutine. No threads are needed for concurrent I/O — only blocking CPU work requires thread offloading.

**asyncio.CancelledError.** The exception raised in a coroutine when `task.cancel()` is called on the containing task. Every coroutine in this codebase that holds a resource (WebSocket, file handle, sounddevice stream) catches it in a `finally` block to ensure cleanup.

**asyncio.Event.** A synchronization primitive that holds a boolean flag. `event.set()` marks it true; `event.clear()` marks it false; `await event.wait()` blocks until it is set. Used in this project for `utterance_event` and `interrupt_event`.

**asyncio.Queue.** A first-in, first-out buffer safe for use within a single event loop. `put_nowait()` adds an item without blocking; `get()` is a coroutine that waits for an item. Used for `mic_queue`, `token_queue`, `audio_queue`, and `transcript_queue`.

**asyncio.Task.** A scheduled coroutine managed by the event loop. Created with `asyncio.create_task()`. Can be cancelled with `task.cancel()`, which delivers `CancelledError` to the coroutine's current `await` point.

**Barge-in.** The act of the user interrupting the agent while it is speaking. Also called turn-taking interruption. The interrupt path in this application fires `interrupt_event` after detecting 100ms of sustained speech during SPEAKING, then cancels the agent, TTS, and playback tasks.

**Buffer underrun.** A condition where a consumer (the audio playback loop) drains a queue faster than the producer (ElevenLabs streaming) fills it, resulting in a silence gap. In this application it manifests as a pause mid-response when one ElevenLabs phrase finishes playing before the next phrase's HTTP response has arrived.

**call_soon_threadsafe.** An asyncio method that schedules a callback to run on the event loop from a different thread. The only safe way to produce asyncio queue items from a PortAudio callback thread.

**CancelledError.** See asyncio.CancelledError.

**Ed25519.** An elliptic-curve digital signature algorithm. Used here to authenticate the voice interface as a trusted device to the OpenClaw Gateway. The private key is a 32-byte scalar; the public key is a 32-byte point; the signature is 64 bytes.

**Endpointing.** The process of detecting when a speaker has finished an utterance. In this application, endpointing is handled entirely by Deepgram: after `DEEPGRAM_ENDPOINTING_MS` milliseconds of continuous silence, Deepgram emits a final transcript.

**FSM (Finite State Machine).** A computational model with a finite set of states, transitions between them triggered by events, and actions taken on entry or exit. In this project, the FSM has three states (LISTENING, THINKING, SPEAKING) and four transitions.

**is_final.** Deepgram's terminology for a finalized transcript result. Opposed to interim (partial) results. Only `is_final=True` results are forwarded upstream by `stt.py`.

**Keepalive.** A periodic message sent over an open connection to prevent the remote end from closing it due to inactivity. In this application, `_send_keepalives()` sends `{"type": "KeepAlive"}` to Deepgram every 8 seconds to stay within their 10-second idle timeout.

**PKCS8.** A standard format for storing private keys, including the key type identifier. Used to persist the Ed25519 device key on disk in PEM-encoded form at `~/.openclaw/voice-device-key.pem`.

**PCM (Pulse-Code Modulation).** Raw uncompressed audio data: a sequence of sample values representing the amplitude of the audio waveform at uniform time intervals. In this application, PCM is int16 (signed 16-bit), mono, at 16kHz — each sample is 2 bytes, each second of audio is 32,000 bytes.

**Persistent connection.** A WebSocket connection opened once and reused across multiple operations, as opposed to a per-operation connection. The STT connection uses this pattern to eliminate the per-turn Deepgram handshake latency.

**PortAudio.** A cross-platform audio I/O library that sounddevice wraps. PortAudio manages audio device enumeration, sample format conversion, and callback thread management at the C level.

**RMS (Root Mean Square).** A measure of the energy of an audio signal, computed as the square root of the mean of squared sample values. Used in `_rms()` as a fast amplitude gate before the more expensive webrtcvad call.

**Sentinel.** A special value placed in a queue to signal termination to a consumer. In this application, `None` is the sentinel in `token_queue`: when `_run_agent()` finishes (normally or via cancellation), it puts `None` in the queue; `_token_source()` in `_run_tts()` breaks when it reads `None`.

**Sounddevice.** A Python binding to PortAudio that provides an asyncio-compatible interface for audio capture and playback. Used for both the microphone input stream (`InputStream`) and the speaker output stream (`OutputStream`).

**STT (Speech-to-Text).** The conversion of raw audio into a text transcript. In this application, performed by Deepgram's `nova-2` model over a streaming WebSocket.

**TTFB (Time to First Byte).** In the context of streaming HTTP/WebSocket APIs, the latency from sending a request to receiving the first byte of the response. Used to describe ElevenLabs response latency.

**TTFT (Time to First Token).** The latency from sending a prompt to a language model to receiving the first output token. The dominant variable in the voice pipeline's end-to-end latency.

**TTS (Text-to-Speech).** The conversion of text into audio. In this application, performed by ElevenLabs' streaming API using the `eleven_turbo_v2_5` model.

**Transition lock.** An `asyncio.Lock` that serializes state transitions. If two transition requests arrive concurrently (e.g., interrupt fires while THINKING→SPEAKING is in progress), the second request blocks on the lock until the first completes, then checks if the state has already moved past the target state.

**Token queue.** The `asyncio.Queue[Optional[str]]` that carries text tokens from the agent to the TTS engine. Contains text strings during normal operation and a single `None` sentinel when the agent stream closes.

**VAD (Voice Activity Detection).** Algorithmic classification of audio frames as speech or silence. In this application, performed by `webrtcvad` at aggressiveness level 2, gated by an RMS energy threshold.

**webrtcvad.** A Python binding to Google's WebRTC Voice Activity Detection algorithm. Operates on 10ms, 20ms, or 30ms PCM frames and classifies them as speech or silence. Stateless per-frame classifier.

**WebSocket.** A full-duplex communication protocol over a single TCP connection. Used by Deepgram (STT), ElevenLabs (TTS), and OpenClaw (agent) for streaming data.

---

*End of BUILD_LOG.md*
