Here is the full diagnostic report. All observations reference specific file  
  locations.                 
                                                                                  ---                                                                             1. LATENCY BREAKDOWN                                                          
                                                                                
  Full path from user finishing a sentence to first audio word:

  ┌─────────────────────┬──────────────────────────────────┬───────────────┐    
  │        Stage        │            Mechanism             │   Estimate    │    
  ├─────────────────────┼──────────────────────────────────┼───────────────┤    
  │                     │ config.py:51 —                   │               │    
  │ Deepgram            │ deepgram_endpointing_ms=60 —     │ 60ms          │    
  │ endpointing         │ silence required before Deepgram │               │    
  │                     │  finalizes                       │               │    
  ├─────────────────────┼──────────────────────────────────┼───────────────┤    
  │ Deepgram network    │ Processing + WS round-trip on    │               │    
  │ RTT                 │ persistent connection (already   │ 50–150ms      │    
  │                     │ open)                            │               │    
  ├─────────────────────┼──────────────────────────────────┼───────────────┤    
  │ Polling delay:      │ state.py:136 — 50ms sleep; up to │ 0–50ms        │    
  │ utterance→THINKING  │  one full cycle                  │               │    
  ├─────────────────────┼──────────────────────────────────┼───────────────┤    
  │ FSM teardown        │ Cancel + await capture_task,     │ 5–20ms        │    
  │ (mic+stt tasks)     │ stt_task; close PortAudio stream │               │    
  ├─────────────────────┼──────────────────────────────────┼───────────────┤    
  │ WebSocket connect   │ Loopback TCP + TLS upgrade:      │               │    
  │ to OpenClaw         │ agent.py:180                     │ 5–15ms        │    
  │                     │ websockets.connect()             │               │    
  ├─────────────────────┼──────────────────────────────────┼───────────────┤    
  │ Gateway auth        │ 3 WS messages: challenge →       │               │    
  │ handshake           │ connect req → hello-ok           │ 5–20ms        │    
  │                     │ (agent.py:183–226)               │               │    
  ├─────────────────────┼──────────────────────────────────┼───────────────┤    
  │ First token from    │ LLM TTFT behind the gateway —    │ 200ms–3s+     │    
  │ OpenClaw            │ model-dependent                  │               │    
  ├─────────────────────┼──────────────────────────────────┼───────────────┤    
  │ Polling delay:      │ state.py:149 not                 │               │    
  │ first               │ token_queue.empty() — another    │ 0–50ms        │    
  │ token→SPEAKING      │ 50ms poll cycle                  │               │    
  ├─────────────────────┼──────────────────────────────────┼───────────────┤    
  │                     │ tts.py:31 _MIN_BUFFER_TOKENS=5 — │ 100–500ms at  │    
  │ TTS token buffering │  waits for 5 tokens OR           │ typical token │    
  │                     │ punctuation before first         │  rates        │    
  │                     │ ElevenLabs call                  │               │    
  ├─────────────────────┼──────────────────────────────────┼───────────────┤    
  │ ElevenLabs TTFB     │ First chunk from                 │ 200–400ms     │    
  │                     │ eleven_turbo_v2_5 streaming      │               │    
  ├─────────────────────┼──────────────────────────────────┼───────────────┤    
  │ Total perceived     │                                  │ ~630ms–4.2s+  │    
  │ silence             │                                  │               │    
  └─────────────────────┴──────────────────────────────────┴───────────────┘    

  Biggest gap: LLM TTFT from OpenClaw. The entire pipeline from
  user-stops-speaking to first-audio-playing is dominated by whatever model     
  OpenClaw is routing to. Everything else is well-optimized; this stage is      
  completely outside the voice interface's control and can dwarf all other      
  stages combined.

  Second biggest gap: TTS buffering. The THINKING→SPEAKING transition fires on  
  the first token, but no audio is produced until 5 tokens have accumulated AND 
  ElevenLabs has responded. If the agent produces tokens at 30 tok/s, buffering 
  alone adds ~167ms before ElevenLabs even starts.

  Dead config value: config.py:46 silence_duration_ms=600 is never referenced   
  anywhere in the codebase. It appears to be a leftover from a local-VAD        
  endpointing design that was superseded by Deepgram's endpointing_ms. There is 
  no local silence counter; the 60ms Deepgram value is the only one that        
  matters.

  ---
  2. INTERRUPT QUALITY

  When does interrupt_event fire?
  state.py:416: await asyncio.sleep(2.0) — a hard-coded 2.0-second delay before 
  VAD watcher even begins checking frames. After the delay, state.py:428        
  requires 5 consecutive speech frames (5 × 20ms = 100ms) of sustained speech.  
  Earliest possible interrupt detection: 2.1 seconds after SPEAKING starts. The 
  user must wait at least 2 seconds into any response before a barge-in has any 
  effect.

  How long does task teardown take?
  After interrupt is detected by the main loop (0–50ms polling at state.py:153),
   _transition_to(LISTENING) cancels 5 tasks: mic_capture_speaking, tts_task,   
  playback_task, vad_task, _agent_task. Estimated time: 10–30ms.

  Notable: play() at audio.py:217 checks interrupt_event.is_set() at the top of 
  the while loop. If it is currently inside await loop.run_in_executor(None,    
  stream.write, audio_array) at audio.py:226, that blocking call in the thread  
  pool cannot be cancelled mid-write. The task cancellation (from FSM teardown) 
  won't land until after run_in_executor returns. One chunk of audio (~20ms)    
  plays after the interrupt fires before the playback task actually stops.      

  When does the new STT sender start accepting frames?
  _transition_to(LISTENING) does NOT drain mic_queue (state.py:241 —
  intentional). After teardown completes, _start_listening() creates a new      
  start_capture task and stt_task. These start on the next event loop tick after
   _transition_to returns. The gap between the old PortAudio stream closing and 
  the new one opening is approximately 1–5ms.

  Are mic frames dropped in the gap?
  Minimal. Frames produced in the ~1–5ms gap are lost, representing 0–1 audio   
  frames. The 100ms of speech that triggered the interrupt
  (_watch_for_interrupt's 5-frame requirement) is preserved in mic_queue and    
  delivered to the new sender immediately.

  Unaddressed edge case — queue backlog during 2s delay:
  During the 2.0-second VAD delay, mic_capture_speaking is running and feeding  
  frames to mic_queue. _watch_for_interrupt is sleeping and not consuming from  
  it. The queue accumulates up to 100 frames (2s ÷ 20ms/frame). When the VAD    
  watcher wakes at t=2.0s, it immediately processes all backlogged frames. If   
  the user spoke during that window (e.g. at t=1.5s), the backlogged speech     
  frames appear as an instant burst at t=2.0s and trigger interrupt detection   
  essentially immediately. The 2-second protection thus does not guarantee the  
  system is deaf for 2 seconds — it guarantees the DETECTION is delayed by up to
   2 seconds, after which any queued speech is processed at full speed.

  ---
  3. STT ACCURACY ISSUES

  Risk of partial transcripts being sent to OpenClaw?
  The is_final filter at stt.py:239 prevents interim results from reaching the  
  agent. However, Deepgram's endpointing_ms=60 is very aggressive. A natural    
  mid-sentence pause longer than 60ms (common in thinking pauses like "I want   
  to... ask about the weather") causes Deepgram to finalize "I want to" as a    
  complete utterance. The sentence is split: "I want to" goes to OpenClaw, "ask 
  about the weather" is finalized as a second utterance, then queued separately.

  What happens if Deepgram returns multiple final transcripts for a single      
  utterance?
  This is a correctness bug. Trace:

  1. User says "I want to... ask about the weather" — mid-sentence pause > 60ms 
  2. Deepgram finalizes "I want to" → stt._transcript_queue gets it → stream()  
  yields it → _run_stt() puts it on state.transcript_queue
  3. Deepgram then finalizes "ask about the weather" → same path →
  state.transcript_queue now holds ["I want to", "ask about the weather"]       
  4. utterance_event fires; main loop gets the FIRST item: "I want to" → sends  
  to OpenClaw
  5. "ask about the weather" sits in state.transcript_queue
  6. Next LISTENING period: new stream() call drains stt._transcript_queue      
  (Deepgram's internal queue — stt.py:273) but does NOT drain
  state.transcript_queue
  7. User says "what is 2 plus 2" → new Deepgram result puts that on
  state.transcript_queue AFTER the stale entry
  8. utterance_event fires → state.transcript_queue.get() returns "ask about the
   weather" (the stale second half from the previous turn), not "what is 2 plus 
  2"

  The stale transcript from the previous turn's second fragment is sent to      
  OpenClaw. The user's actual question is discarded until the following turn.   

  What happens during the STT sender gap between utterances?
  Between LISTENING cancellation and next LISTENING entry, the Deepgram
  connection is open but no audio is being sent (sender is cancelled). The      
  keepalive task at stt.py:206 fires every 8 seconds. Deepgram may produce a    
  stale finalization based on buffered audio from the tail of the previous turn.
   _on_persistent_message enqueues it. stream() drains stt._transcript_queue at 
  its entry point (stt.py:273–274), so these stale results are flushed. However,
   the stale entries in state.transcript_queue described above are NOT flushed —
   the drain in stream() only flushes Deepgram's internal queue.

  ---
  4. TTS CONTINUITY

  If ElevenLabs is slow on one chunk:
  _stream_phrase applies a timeout only to the first chunk (tts.py:93).
  Subsequent chunks have no timeout (tts.py:108). A slow second-through-Nth     
  chunk starves audio_queue. play() at audio.py:219 polls with a 50ms timeout:  
  on empty queue, it catches TimeoutError, loops back, and does nothing. The    
  PortAudio output stream (sd.OutputStream) is left idle. PortAudio behavior on 
  output underrun is driver-dependent — it typically outputs silence or the     
  previous buffer, which produces an audible gap or click. There is no
  pre-buffering to absorb ElevenLabs jitter.

  If the agent produces a very short response (1–2 tokens):
  Handled correctly. tts.py:149–152: after the token stream closes (None        
  sentinel), any remaining buffer is flushed regardless of size. A single-token 
  response like "OK" with no punctuation reaches synthesize()'s final flush. A  
  2-token "OK." triggers the punctuation flush on the second token (stripped[-1]
   in _FLUSH_PUNCTUATION). No functional issue.

  Buffer underrun risk:
  Yes, structural. TTS processes phrases serially: synthesize() calls
  _stream_phrase() with async for chunk in self._stream_phrase(joined): yield   
  chunk — one ElevenLabs HTTP call must complete before the next begins. A      
  multi-sentence response produces multiple sequential ElevenLabs calls. If     
  phrase 1 finishes playing before phrase 2's ElevenLabs response arrives,      
  audio_queue empties and playback pauses. The tts_done_event is not set until  
  ALL phrases are done (state.py:388), so the FSM does not declare completion — 
  but the user hears a silence gap mid-response.

  ---
  5. OPENCLAW SUBPROCESS

  There is no subprocess. agent.py is a WebSocket client. config.py:69
  openclaw_cmd is marked "not used by WS approach." All subprocess-related      
  failure modes are N/A. The relevant failure modes for the WS approach:        

  OpenClaw takes a very long time (or hangs) on first token:
  agent.py:253 async for raw in ws: has no timeout. If OpenClaw accepts the     
  request but never produces an assistant stream event, the _run_agent coroutine
   blocks indefinitely on the websockets iterator. token_queue stays empty      
  forever. The main loop at state.py:149 keeps polling not
  self.token_queue.empty() every 50ms indefinitely. The pipeline is stuck in    
  THINKING with no escape except Ctrl+C or an uncaught WebSocket error.

  The same applies if OpenClaw only produces lifecycle events without any       
  assistant events — technically valid protocol behavior if the agent decides   
  not to respond.

  WebSocket exits with error before completing:
  agent.py:278: any exception yields _ERROR_UTTERANCE. The None sentinel is sent
   by _run_agent's finally block (state.py:363). This triggers
  THINKING→SPEAKING, TTS synthesizes the error phrase, it plays, then natural   
  completion returns to LISTENING. This path is handled correctly.

  OpenClaw daemon not running:
  websockets.connect() raises ConnectionRefusedError. Caught by except Exception
   as exc, error phrase yielded. Handled cleanly.

  Zombie WebSocket connections after interrupt:
  When _agent_task is cancelled, agent.py:272 catches CancelledError and exits. 
  The async with websockets.connect(self._ws_url) as ws: context manager's      
  __aexit__ closes the WebSocket cleanly. No orphaned connections.

  The auth handshake has no timeout:
  agent.py:183: raw = await ws.recv() — no timeout waiting for the challenge.   
  agent.py:221: raw = await ws.recv() — no timeout waiting for hello-ok.        
  If the Gateway is running but unresponsive, these block indefinitely.

  ---
  6. CONVERSATION QUALITY

  Perceived silence gap:
  As detailed in §1, the gap from user-stops-talking to first audio is
  630ms–4.2s+. The dominant variable is LLM TTFT. On a fast model
  (GPT-4o-level), the gap is noticeable but acceptable (~700–900ms). On a slower
   model or under load, it is conspicuous.

  Does the agent have context about previous turns?
  No. agent.py sends only the current transcript to OpenClaw with no sessionId  
  parameter. Each turn is a fresh request to agentId: "main". Unless OpenClaw's 
  "main" agent maintains its own persistent memory independently (which it may  
  do via its internal scaffolding), the voice interface cannot reference        
  anything said in prior turns. A user asking "can you explain that again?"     
  receives a response with no referent.

  What if the user speaks before fully back in LISTENING?
  The FSM drain logic correctly preserves frames: mic_queue is not drained on   
  LISTENING entry (state.py:238–241). If the user speaks during the
  SPEAKING→LISTENING teardown (20–50ms), those frames are buffered and delivered
   to the new sender task. Functionally correct.

  The practical constraint is the 2-second barge-in delay. A user who speaks 1.5
   seconds into the agent's response must wait another 600ms before the
  interrupt fires (~100ms speech detection after the 2s window opens). First    
  1.5s of their speech is captured in mic_queue but not acted on. After
  interrupt + teardown, all those buffered frames are sent to Deepgram. Deepgram
   receives a burst of audio rather than a real-time stream, which may affect   
  finalization timing.

  ---
  7. RESILIENCE

  Deepgram drops connection mid-utterance:
  stt.py:296–302: stream() uses a 5.0-second timeout on _transcript_queue.get().
   On timeout, it checks _listener_task.done(). If the listener has exited      
  (connection dropped), it calls utterance_event.set(), yields "" (empty        
  string), and returns. _run_stt puts "" on state.transcript_queue. The FSM     
  receives an empty transcript and sends it to OpenClaw. OpenClaw responds to an
   empty message (behavior undefined — likely a confused response). The 5-second
   wait is the user-visible silence before this fallback activates.

  ElevenLabs returns 429 (rate limit):
  The ElevenLabs SDK raises an exception on 429. In _stream_phrase, this        
  propagates through aiter.__anext__() inside asyncio.wait_for and is NOT caught
   there — it surfaces in synthesize() at tts.py:157: except Exception as exc:  
  logger.warning(...). The generator exits silently. _run_tts reaches the end of
   the async for loop over synthesize() and sets tts_done_event. audio_queue is 
  empty. The main loop detects audio_queue.empty() AND tts_done_event →
  SPEAKING→LISTENING transition. The user hears absolute silence when a rate    
  limit occurs. No spoken error, no indication of failure.

  OpenClaw daemon not running:
  websockets.connect() raises ConnectionRefusedError before any auth.
  agent.py:278 catches it, yields _ERROR_UTTERANCE. The error phrase is spoken. 
  Handled correctly.

  User stays silent for 60 seconds:
  stream() polls with a 5-second timeout. Each 5-second cycle catches
  TimeoutError, checks _listener_task.done() (if alive, continues), loops. The  
  stt_keepalive task fires every 8 seconds to prevent Deepgram's 10-second idle 
  timeout. But the sender IS active, feeding silent frames to Deepgram. Deepgram
   receives continuous audio (silence), so it likely won't close the connection 
  from its side. System stays in LISTENING indefinitely. Correct behavior.      

  ---
  Summary of Issues by User Impact

  ┌──────────┬──────────────────────────────────────────┬───────────────────┐   
  │ Priority │                  Issue                   │     Location      │   
  ├──────────┼──────────────────────────────────────────┼───────────────────┤   
  │ Critical │ THINKING hangs indefinitely if OpenClaw  │ agent.py:253      │   
  │          │ stalls — no timeout on agent streaming   │                   │   
  ├──────────┼──────────────────────────────────────────┼───────────────────┤   
  │          │ Stale transcript race:                   │                   │   
  │ Critical │ state.transcript_queue not drained       │ state.py:141      │   
  │          │ between turns; previous turn's split     │                   │   
  │          │ fragment sent as next turn's input       │                   │   
  ├──────────┼──────────────────────────────────────────┼───────────────────┤   
  │ High     │ 2.0-second hard delay before any         │ state.py:416      │   
  │          │ barge-in is possible                     │                   │   
  ├──────────┼──────────────────────────────────────────┼───────────────────┤   
  │ High     │ ElevenLabs 429 silently produces no      │ tts.py:154–158    │   
  │          │ audio and no error feedback to user      │                   │   
  ├──────────┼──────────────────────────────────────────┼───────────────────┤   
  │ High     │ No conversation history — agent has no   │ agent.py:232–246  │   
  │          │ context of prior turns                   │                   │   
  ├──────────┼──────────────────────────────────────────┼───────────────────┤   
  │ High     │ Buffer underrun: serial ElevenLabs calls │ tts.py:145,       │   
  │          │  cause mid-response silence gaps         │ audio.py:219      │   
  ├──────────┼──────────────────────────────────────────┼───────────────────┤   
  │          │ 60ms Deepgram endpointing splits         │                   │   
  │ Medium   │ natural-pause sentences into multiple    │ config.py:51      │   
  │          │ utterances                               │                   │   
  ├──────────┼──────────────────────────────────────────┼───────────────────┤   
  │ Medium   │ silence_duration_ms=600 is a dead config │ config.py:46      │   
  │          │  value — never read                      │                   │   
  ├──────────┼──────────────────────────────────────────┼───────────────────┤   
  │          │ Auth handshake has no timeout (ws.recv() │                   │   
  │ Medium   │  for challenge and hello-ok can block    │ agent.py:183, 221 │   
  │          │ forever)                                 │                   │   
  ├──────────┼──────────────────────────────────────────┼───────────────────┤   
  │ Low      │ Two 50ms polling cycles in the critical  │ state.py:136      │   
  │          │ latency path                             │                   │   
  ├──────────┼──────────────────────────────────────────┼───────────────────┤   
  │          │ Deepgram drop falls back with 5-second   │                   │   
  │ Low      │ timeout then sends empty string to       │ stt.py:296–302    │   
  │          │ OpenClaw                                 │                