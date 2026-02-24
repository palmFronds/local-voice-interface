"""LLM agent for Simple Voice Interface.

Phase 1 (commented out below): streaming OpenAI-compatible API call.
Phase 2 (active): WebSocket client to the OpenClaw Gateway at LLM_WS_URL.

The public interface is identical in both phases:
    async def run(self, transcript: str) -> AsyncIterator[str]

state.py, tts.py, and main.py never change between phases.
OpenClaw owns conversation history — the voice interface sends only the
current transcript per turn.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import AsyncIterator

import websockets
from websockets.exceptions import WebSocketException

# ── Phase 1 imports (commented out) ──────────────────────────────────────────
# from openai import AsyncOpenAI

from config import Config, load

logger = logging.getLogger(__name__)

# Spoken when the agent call fails. A plain string rather than a raised
# exception so the voice loop always has something to say — a silent failure
# leaves the user with no feedback and the pipeline in an ambiguous state.
_ERROR_UTTERANCE = "Sorry, I ran into an issue. Please try again."


class LLMAgent:
    """OpenClaw Gateway WebSocket client implementing the voice pipeline agent interface.

    Phase 2: opens a fresh WebSocket connection per turn, sends the transcript,
    and yields response tokens as they arrive — preserving the sub-200ms
    time-to-first-audio goal of Phase 1's streaming approach.
    """

    def __init__(self, config: Config) -> None:
        """Initialise LLMAgent.

        Args:
            config: Frozen Config instance from config.load().
        """
        # Phase 2: only the Gateway URL is needed; history lives in OpenClaw.
        self._ws_url: str = config.llm_ws_url

        # ── Phase 1 (commented out) ───────────────────────────────────────────
        # self._client = AsyncOpenAI(
        #     api_key=config.llm_api_key,
        #     base_url=config.llm_api_base,
        # )
        # self._system_prompt: str = config.llm_system_prompt
        # self._history: list[dict[str, str]] = []

    async def run(self, transcript: str) -> AsyncIterator[str]:
        """Stream response tokens for the given user transcript.

        Opens a fresh connection per turn so each call is fully independent.
        Yields tokens as they arrive from the Gateway. On CancelledError the
        async-with block closes the socket cleanly before returning, so
        OpenClaw sees a normal peer disconnect rather than a half-open socket.

        Args:
            transcript: The user's speech-to-text output for this turn.

        Yields:
            Response text tokens in the order the Gateway produces them.
        """
        try:
            async with websockets.connect(self._ws_url) as ws:
                await ws.send(json.dumps({"type": "transcript", "text": transcript}))
                logger.info("Sent transcript to OpenClaw: %r", transcript)

                async for raw in ws:
                    data: dict = json.loads(raw)
                    msg_type = data.get("type")

                    if msg_type == "token":
                        yield data["text"]
                    elif msg_type == "done":
                        break
                    # anything else → ignore (forward-compatible with protocol extensions)

        except asyncio.CancelledError:
            # VoiceStateMachine cancelled this task (user interrupted).
            # async-with __aexit__ has already closed the WebSocket.
            # Do not re-raise — the generator returns via StopAsyncIteration
            # and the teardown sequence handles task lifecycle regardless.
            logger.info("Agent task cancelled")

        except (WebSocketException, OSError) as exc:
            # Connection refused, dropped, or protocol error.
            logger.warning("Agent connection error: %s", exc)
            yield _ERROR_UTTERANCE

        # ── Phase 1 (commented out) ───────────────────────────────────────────
        # try:
        #     self._history.append({"role": "user", "content": transcript})
        #     messages = [{"role": "system", "content": self._system_prompt}, *self._history]
        #     full_response: list[str] = []
        #     async with await self._client.chat.completions.create(
        #         model=self._config.llm_model, messages=messages, stream=True
        #     ) as stream:
        #         async for chunk in stream:
        #             token: str | None = chunk.choices[0].delta.content
        #             if token:
        #                 full_response.append(token)
        #                 yield token
        # except asyncio.CancelledError:
        #     logger.info("Agent task cancelled")
        # except Exception as exc:
        #     logger.error("LLM error: %s", exc)
        #     yield _ERROR_UTTERANCE
        # finally:
        #     if full_response:
        #         self._history.append({"role": "assistant", "content": "".join(full_response)})
        #         max_messages = self._config.llm_max_history_turns * 2
        #         if len(self._history) > max_messages:
        #             self._history = self._history[-max_messages:]
        #     elif self._history and self._history[-1]["role"] == "user":
        #         self._history.pop()


if __name__ == "__main__":
    cfg = load()
    logging.basicConfig(
        level=getattr(logging, cfg.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    async def _test() -> None:
        agent = LLMAgent(cfg)
        print(f"Connecting to OpenClaw at {cfg.llm_ws_url} …")
        print("(Start openclaw_stub.js first: node openclaw_stub.js)\n")

        async for token in agent.run("What is the speed of light? One sentence."):
            print(token, end="", flush=True)
        print()

    asyncio.run(_test())
