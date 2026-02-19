### modular state machine flow

The state machine is an async event-driven FSM (Finite State Machine) with three states and four transitions. Each state owns a set of active coroutines, and every transition involves tearing down the previous state's coroutines before starting the next one's. That ordering — cancel first, then start — is what prevents race conditions.

you stop talking
LISTENING ──────────────────→ THINKING
    ↑                              │
    │                    agent has response
    │                              │
    │          ←────────────────── ↓
    │    you talk          SPEAKING
    └──────────────────────────────┘
         (barge-in interrupt)


### the state machine

LISTENING ──[UTTERANCE_COMPLETE]──→ THINKING
  - cancel: VAD loop, STT stream
  - start:  agent task

THINKING ──[RESPONSE_READY]──→ SPEAKING
  - cancel: nothing (agent keeps running to feed TTS)
  - start:  TTS stream, playback loop, VAD (interrupt mode)

SPEAKING ──[PLAYBACK_COMPLETE]──→ LISTENING
  - cancel: TTS stream, playback loop, VAD
  - start:  VAD loop, STT stream

SPEAKING ──[INTERRUPT]──→ LISTENING
  - cancel: agent task, TTS stream, playback loop
  - start:  VAD loop, STT stream