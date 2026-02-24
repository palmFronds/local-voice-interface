/**
 * openclaw_stub.js — OpenClaw Gateway stub for end-to-end testing.
 *
 * Simulates the OpenClaw WebSocket Gateway at ws://127.0.0.1:18789.
 * Accepts transcript messages and streams back a canned reply token-by-token
 * so the full voice pipeline (STT → agent → TTS → speaker) can be validated
 * before the real OpenClaw daemon exists.
 *
 * Setup (one time):
 *   npm install ws
 *
 * Run:
 *   node openclaw_stub.js
 *
 * Protocol (matches agent.py Phase 2):
 *   Voice interface → stub:  { type: "transcript", content: "...", turn_id: "..." }
 *   Stub → voice interface:  { type: "token", content: "word " }  (one per token)
 *                            { type: "done" }                      (signals end of stream)
 *                            { type: "error", message: "..." }     (on failure)
 */

const { WebSocketServer } = require("ws");

const PORT = 18789;

// Delay between tokens in milliseconds. Mirrors realistic LLM generation pace
// and exercises the streaming path through TTS — set to 0 to stress-test rapid tokens.
const TOKEN_DELAY_MS = 60;

// Canned responses keyed on nothing — the stub ignores the actual transcript
// and cycles through these to keep test turns distinguishable from each other.
// Each response is 3–4 sentences, long enough to fill 8–10 seconds of TTS audio
// and give you a comfortable window to test the interrupt path.
const CANNED_RESPONSES = [
  "The voice pipeline is fully connected and everything looks healthy on this end. Your microphone is picking up audio, Deepgram is producing transcripts, and the tokens are flowing through to ElevenLabs without any issues. This is the OpenClaw stub responding, so the actual agent logic hasn't run yet — but the plumbing from your mouth to the speaker is working exactly as it should.",

  "That's a great question, and I want to give you a thorough answer. The short version is that the entire pipeline — speech recognition, the agent gateway, text-to-speech synthesis, and audio playback — is operating as a single continuous stream rather than a sequence of blocking calls. That means you're hearing the first word of my response within a couple hundred milliseconds of your transcript finalising, instead of waiting for the whole reply to generate before anything plays. It's a meaningful difference in how the conversation feels.",

  "If you want to test the interrupt path, now is a good time — just start speaking while I'm still talking and the state machine should catch it. The VAD will detect your voice, set the interrupt event, and the playback loop will stop mid-sentence. The agent task gets cancelled, the audio buffer gets flushed, and the pipeline returns to listening within about two hundred milliseconds. Go ahead and try it a few times to make sure it holds up consistently.",
];

let turnCount = 0;

const wss = new WebSocketServer({ host: "127.0.0.1", port: PORT });

console.log(`[stub] OpenClaw Gateway stub listening on ws://127.0.0.1:${PORT}`);

wss.on("connection", (ws, req) => {
  console.log(`[stub] Client connected from ${req.socket.remoteAddress}`);

  ws.on("message", (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch {
      console.error("[stub] Received non-JSON message — ignoring");
      return;
    }

    if (msg.type !== "transcript") {
      console.warn(`[stub] Unknown message type: ${msg.type}`);
      return;
    }

    const transcript = msg.text ?? "";
    const turnId = msg.turn_id ?? "(no turn_id)";
    console.log(`[stub] Turn ${turnId}: "${transcript}"`);

    // Pick next canned response (cycles round-robin).
    const response = CANNED_RESPONSES[turnCount % CANNED_RESPONSES.length];
    turnCount++;

    // Stream tokens one word at a time with TOKEN_DELAY_MS between each.
    // Split on spaces, re-attach the trailing space so TTS receives natural
    // word-boundary spacing — same token shape the real OpenClaw will produce.
    const tokens = response.split(" ").map((word, i, arr) =>
      i < arr.length - 1 ? word + " " : word
    );

    let i = 0;

    function sendNextToken() {
      // If the client disconnected mid-stream (interrupt), stop cleanly.
      if (ws.readyState !== ws.OPEN) {
        console.log("[stub] Client disconnected mid-stream — stopping");
        return;
      }

      if (i < tokens.length) {
        ws.send(JSON.stringify({ type: "token", text: tokens[i] }));
        i++;
        setTimeout(sendNextToken, TOKEN_DELAY_MS);
      } else {
        ws.send(JSON.stringify({ type: "done" }));
        console.log("[stub] Stream complete");
      }
    }

    sendNextToken();
  });

  ws.on("close", () => {
    console.log("[stub] Client disconnected");
  });

  ws.on("error", (err) => {
    // Log but do not crash — a single bad client should not kill the server.
    console.error("[stub] WebSocket error:", err.message);
  });
});

wss.on("error", (err) => {
  console.error("[stub] Server error:", err.message);
  process.exit(1);
});
