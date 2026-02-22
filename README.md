# simple-voice-interface

A minimal, locally-run, real-time interruptible voice pipeline for the OpenClaw autonomous agent. Speak to an AI, interrupt it mid-sentence, it stops immediately and listens again.

Running locally eliminates per-minute SaaS voice API costs and removes the latency and privacy exposure of routing audio through a third-party orchestration layer. The only spend is token cost on the LLM call itself; everything else runs on your machine.

---

## What It Does

Connects a microphone to a streaming STT engine, routes the transcript to an LLM agent, and speaks the response back through streaming TTS: all in real time. The defining capability is **barge-in interruption**: speak while the agent is talking and it cancels within ~200ms, no ghost audio, no hung tasks.

```
Microphone → VAD → STT → LLMAgent → TTS → Speaker
                ↑_____________________________|
                        interrupt path
```

The pipeline is governed by an explicit three-state FSM (`LISTENING → THINKING → SPEAKING`) with an `asyncio.Lock` on all transitions. Tasks are always cancelled and awaited before new ones start.

---

## Stack

| Layer | Technology |
|---|---|
| Audio I/O | sounddevice — PCM 16-bit mono 16kHz |
| VAD | webrtcvad |
| STT | Deepgram nova-2 streaming WebSocket |
| LLM | OpenAI-compatible streaming API (Phase 1) / OpenClaw Gateway WS (Phase 2) |
| TTS | ElevenLabs streaming, `pcm_16000` direct output |
| Concurrency | asyncio throughout |

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

Required keys: `DEEPGRAM_API_KEY`, `TTS_API_KEY`, `TTS_VOICE_ID`, `LLM_API_KEY`. See `.env.example` for all options including `VAD_AGGRESSIVENESS` and `LLM_MODEL`.

**Run:**

```bash
python main.py
```

---

## Phase 2: OpenClaw

The current agent wraps a direct OpenAI-compatible streaming call. Phase 2 replaces only the agent internals with a WebSocket client to the OpenClaw Gateway at `ws://127.0.0.1:18789`. The interface is unchanged: the rest of the pipeline is unaffected.

---

## Build Status

| # | What | Status |
|---|---|---|
| 1 | Config dataclass, `.env` loading | ✅ |
| 2 | Mic capture, thread-safe sounddevice bridge | ✅ |
| 3 | VAD — stateless frame classification | ✅ |
| 4 | Deepgram streaming STT | ✅ |
| 5 | Streaming LLM agent, conversation history | ✅ |
| 6 | ElevenLabs streaming TTS, PCM output | ✅ |
| 7 | VoiceStateMachine — FSM, transition lock, task ownership | ✅ |
| 8 | End-to-end wiring, first full pipeline run | ✅ |
| 9 | Interrupt hardening — 10/10 consecutive clean cancellations | 🔲 |
| 10 | Error handling, logging, clean Ctrl+C exit | 🔲 |
| 11 | Local web UI — pulsating orb reflecting live FSM state | 🔲 |
| — | Phase 2: OpenClaw Gateway integration | 🔲 |
