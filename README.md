# simple-voice-interface

A minimal, locally-run, real-time interruptible voice pipeline for the OpenClaw autonomous agent. Speak to OpenClaw, interrupt it mid-sentence — it stops within ~200ms and starts listening again.

Running locally eliminates per-minute SaaS voice API costs and removes the latency and privacy exposure of routing audio through a third-party orchestration layer. The only spend is the token cost on the agent side; everything else runs on your machine.

---

## What It Does

Connects a microphone to a streaming STT engine, routes the transcript to the OpenClaw Gateway WebSocket API, and speaks the response back through streaming TTS — all in real time. The defining capability is **barge-in interruption**: speak while the agent is talking and it cancels within ~200ms, no ghost audio, no hung tasks.

```
Microphone → VAD → STT → OpenClaw Gateway → TTS → Speaker
                ↑___________________________________|
                              interrupt path
```

Two architectural decisions eliminate the latency spikes that commonly plague local voice pipelines. First, the PortAudio stream is opened once at startup and runs for the entire session — state transitions never reopen it, so there is no 40–100ms WASAPI re-init gap between turns. Second, the Deepgram WebSocket is opened once at startup and kept warm by a keepalive task — SPEAKING → LISTENING transitions skip the ~400ms TLS+HTTP upgrade entirely.

---

## Stack

| Layer | Technology |
|---|---|
| Audio I/O | sounddevice — PCM 16-bit mono 16 kHz |
| VAD | webrtcvad + RMS energy threshold |
| VAD interrupt | 5 consecutive speech frames (100ms); trigger frames re-queued on interrupt |
| STT | Deepgram nova-2 streaming WebSocket, persistent connection |
| LLM | OpenClaw Gateway WebSocket (phase 2 active) |
| TTS | ElevenLabs eleven_turbo_v2_5, `pcm_16000` direct output |
| Concurrency | asyncio throughout; sounddevice callback bridged via `call_soon_threadsafe` |

---

## Architecture

The system is a single asyncio event loop running five coroutines coordinated by an explicit `VoiceStateMachine`. The FSM owns three states — `LISTENING`, `THINKING`, `SPEAKING` — and every transition acquires a `transition_lock` before cancelling active tasks. This prevents the race condition where a late-arriving interrupt fires while a THINKING → SPEAKING transition is already in progress: the second transition blocks on the lock, then runs as a no-op if the state has already advanced past SPEAKING.

**Persistent PortAudio stream.** On earlier iterations, `start_capture()` was called on every LISTENING and SPEAKING entry. On Windows (WASAPI shared mode), PortAudio takes 40–100ms to deliver its first callback after reopening, swallowing any audio spoken during that window. The current design opens one `InputStream` at startup in `start_persistent_capture()` and never closes it mid-session. All states share the same `mic_queue`; per-state logic decides which frames to act on.

**Persistent Deepgram WebSocket.** A single connection is opened in `StreamingSTT.connect()` and maintained by a keeper task that reconnects with exponential backoff on drops. A KeepAlive message is sent every 8 seconds to prevent Deepgram's 10-second idle timeout from firing during THINKING and SPEAKING when no audio frames are being forwarded. Each LISTENING entry starts only a lightweight `sender_task` that feeds frames into the warm connection — no handshake overhead per turn.

**Interrupt path.** During SPEAKING, `_watch_for_interrupt()` drains `mic_queue` frame by frame and classifies each one with VAD. It requires 5 consecutive speech frames (100ms) before declaring an interrupt, rejecting single-frame noise. Crucially, every frame consumed from the queue — including the pre-speech silence context and the 5 trigger frames themselves — is put back onto `mic_queue` before `interrupt_event` is set. When the transition to LISTENING completes and the new `stt_task` starts, those frames are the first ones forwarded to Deepgram, so the first word of the barge-in utterance is not lost.

---

## Build Status

| Component | Status | Notes |
|---|---|---|
| Config + env loading | ✅ | |
| Persistent mic capture | ✅ | Single PortAudio stream, session lifetime |
| VAD + RMS energy threshold | ✅ | Tunable via `VAD_ENERGY_THRESHOLD` |
| Persistent Deepgram STT | ✅ | Keepalive, no reconnect overhead per turn |
| OpenClaw Gateway agent | ✅ | WebSocket, Ed25519 device-auth, streaming events |
| ElevenLabs streaming TTS | ✅ | PCM direct, phrase-buffered synthesis |
| FSM with transition lock | ✅ | LISTENING → THINKING → SPEAKING |
| Barge-in interruption | ✅ | ~200ms cancel, trigger frames preserved |
| Error handling + timeouts | ✅ | Agent timeout, STT retry, clean Ctrl+C exit |
| Stale transcript fix | ✅ | Queue drained on LISTENING entry |
| Voice naturalness | 🔧 | Occasional clipping, endpointing tuning ongoing |
| Race condition | 🔧 | Rare but present, under investigation |
| Interrupt hardening (10x) | 🔲 | Needs headphone session |
| Conversation history | 🔲 | Each turn stateless, sessionId not passed |

---

## Known Limitations

**Voice naturalness.** TTS synthesis is phrase-buffered: tokens accumulate until a punctuation boundary or a minimum count is reached before a request is sent to ElevenLabs. This produces coherent prosody on full sentences but can sound slightly abrupt on short one- or two-word replies where no sentence-final punctuation triggers a flush. ElevenLabs buffer underrun is also possible on a slow or rate-limited network; the pipeline recovers silently and continues.

**Occasional race condition.** Under specific timing conditions — typically when a Deepgram `is_final` event arrives within the same 50ms polling tick as a state transition — a stale transcript can trigger a spurious LISTENING → THINKING before the user has spoken. This is rare in normal use and is under active investigation.

**Stateless conversation.** Each turn is sent to the OpenClaw Gateway as an independent agent request without a `sessionId`. OpenClaw's main agent session may maintain its own internal context, but the voice pipeline does not explicitly thread session continuity across turns, so multi-turn reasoning relies entirely on what the agent retains on its side.

**Deepgram endpointing.** The endpointing window is set to 300ms, which handles most natural pauses. Very long hesitations or deliberate mid-sentence pauses longer than 300ms may still cause Deepgram to emit an early `is_final` that splits a single sentence into two fragments, of which only the first reaches the agent.

---

## Setup

**Prerequisites:** Python 3.11+. Windows users must enable Long Path support before installing dependencies:

```powershell
# Run as Administrator
New-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem" `
  -Name "LongPathsEnabled" -Value 1 -PropertyType DWORD -Force
```

**Install:**

```bash
pip install -r requirements.txt
```

**Configure:**

```bash
cp .env.example .env
# Edit .env with your API keys
```

Required keys: `DEEPGRAM_API_KEY`, `TTS_API_KEY`, `TTS_VOICE_ID`. The gateway token (`OPENCLAW_GATEWAY_TOKEN`) is read from your local OpenClaw installation at `~/.openclaw/openclaw.json → gateway.auth.token`. `OPENCLAW_CMD` defaults to `openclaw` on your system `PATH` and only needs to be set if your installation is non-standard. See `.env.example` for all tunables including `VAD_AGGRESSIVENESS`, `SILENCE_DURATION_MS`, and `DEEPGRAM_ENDPOINTING_MS`.

**Run:**

```bash
python main.py
```

OpenClaw must be running as a daemon before starting the voice interface. The agent connects to `ws://127.0.0.1:18789` on each turn and will log a connection error if the gateway is not available.

### Testing without OpenClaw

`openclaw_stub.js` is a Node.js WebSocket server that listens on `ws://127.0.0.1:18789` and streams canned responses token-by-token. It is useful for verifying the full voice pipeline — mic capture, VAD, STT, TTS, and speaker playback — without the OpenClaw daemon running. Note that the stub implements a simplified protocol and does not exercise the full Gateway Ed25519 device-auth handshake used by the production agent.

```bash
# One-time setup
npm install ws

# Start the stub (keep running in a separate terminal)
node openclaw_stub.js

# Start the voice interface
python main.py
```

---

## Building a Standalone Exe

The project packages as a Windows `.exe` via PyInstaller. API keys are baked into the binary at build time from the environment — they never appear in source control.

**Prerequisites:** `pip install pyinstaller`

**Steps:**

1. Fill in `.env` with your API keys (copy from `.env.example` if needed):
   ```
   DEEPGRAM_API_KEY=...
   TTS_API_KEY=...
   TTS_VOICE_ID=...
   OPENCLAW_GATEWAY_TOKEN=...
   OPENCLAW_CMD=C:\path\to\openclaw.cmd
   ```

2. Build:
   ```bash
   pyinstaller build.spec --clean
   ```
   PyInstaller reads the keys from `.env` (via the normal `load_dotenv()` path in `config.py`) and bakes whatever values are present into the frozen bytecode. The resulting exe needs no `.env` file on the target machine.

3. Output is at `dist/openclaw-voice/openclaw-voice.exe`. Zip for distribution:
   ```powershell
   Compress-Archive -Path dist\openclaw-voice -DestinationPath dist\openclaw-voice.zip
   ```

**Never commit `.env` or `dist/` — both are listed in `.gitignore`.**
