"""LLM agent for Simple Voice Interface — Phase 1.

Phase 1 wraps a direct streaming OpenAI-compatible API call.
Phase 2 (deferred) replaces these internals with a WebSocket client that
connects to the OpenClaw Gateway at ws://127.0.0.1:18789. The run()
interface is identical in both phases so state.py, tts.py, and main.py
never need to know which backend is active.
"""

from __future__ import annotations

import asyncio
import logging
from typing import AsyncIterator

from openai import AsyncOpenAI

from config import Config, load

logger = logging.getLogger(__name__)

# Spoken when the LLM call fails. A plain string rather than a raised
# exception because the voice layer must always have something to say —
# a silent failure leaves the user with no feedback and the pipeline in
# an ambiguous state. TTS speaks this token just like any normal response.
_ERROR_UTTERANCE = "Sorry, I ran into an issue. Please try again."


class LLMAgent:
    """Streaming LLM wrapper implementing the voice pipeline agent interface.

    Phase 1: streaming OpenAI-compatible API call (stream=True). Tokens are
    yielded as the model produces them so TTS can begin speaking within ~200ms
    of the first token — the key latency improvement over Vocalis's blocking
    requests.post() approach, which yields nothing until the full response is done.

    Phase 2 (deferred): replace __init__ and run() internals with a WebSocket
    client to the OpenClaw Gateway. The public interface is unchanged.
    """

    def __init__(self, config: Config) -> None:
        """Initialise LLMAgent.

        Args:
            config: Frozen Config instance from config.load().
        """
        self._config = config
        # AsyncOpenAI is stateless at construction — only credentials and the
        # base URL are stored. No network I/O happens here.
        self._client = AsyncOpenAI(
            api_key=config.llm_api_key,
            base_url=config.llm_api_base,
        )
        self._system_prompt: str = config.llm_system_prompt
        # Stores alternating user/assistant turns. The system prompt is NOT
        # stored here — it is prepended fresh on every call (same approach as
        # Vocalis) so it survives history trimming and is always at position 0
        # of the messages array sent to the API.
        self._history: list[dict[str, str]] = []

    async def run(self, transcript: str) -> AsyncIterator[str]:
        """Stream response tokens for the given user transcript.

        Streams response tokens as they are generated. Handles cancellation
        cleanly. Yields an error string on failure rather than raising.

        Derived from Vocalis's get_response() message-building pattern:
        user message appended to history first, then messages = [system] + history.
        Differs by using stream=True and yielding tokens as they arrive instead
        of blocking on requests.post() until the full response is ready.

        Args:
            transcript: The user's speech-to-text output for this turn.

        Yields:
            Response text tokens in the order the model produces them.
        """
        # Append user turn first — then build messages from history.
        # Vocalis does the same: add_to_history("user", user_input) then
        # messages.extend(self.conversation_history). The system prompt is
        # prepended separately so it always leads the messages array.
        self._history.append({"role": "user", "content": transcript})

        messages: list[dict[str, str]] = [
            {"role": "system", "content": self._system_prompt},
            *self._history,
        ]

        full_response: list[str] = []

        try:
            async with await self._client.chat.completions.create(
                model=self._config.llm_model,
                messages=messages,
                stream=True,
            ) as stream:
                async for chunk in stream:
                    token: str | None = chunk.choices[0].delta.content
                    if token:
                        full_response.append(token)
                        yield token

        except asyncio.CancelledError:
            # VoiceStateMachine cancelled this task (user interrupted).
            # The async with __aexit__ already closed the HTTP stream.
            # We do not re-raise: the generator returns normally via
            # StopAsyncIteration, and the teardown sequence handles the
            # task lifecycle regardless of whether CancelledError propagates.
            logger.info("Agent task cancelled")

        except Exception as exc:
            logger.error("LLM error: %s", exc)
            # Yield the error string so TTS has something to speak.
            # Raising here would propagate through the async for in state.py
            # and crash the pipeline; a spoken error keeps the loop alive.
            yield _ERROR_UTTERANCE

        finally:
            if full_response:
                # Commit whatever tokens accumulated — full response on clean
                # finish, partial on mid-stream cancellation. A partial response
                # is better than leaving an unmatched user message in history,
                # which would violate the alternating user/assistant pattern the
                # API expects.
                self._history.append(
                    {"role": "assistant", "content": "".join(full_response)}
                )
                # Rolling trim: keep the most recent turns that fit in the window.
                # Derived from Vocalis's trimming logic in add_to_history():
                # they keep history[-49:] (plus system at [0]). We keep the last
                # max_messages entries. The system prompt is NOT in self._history
                # so there is no index-0 guard needed here — it is always
                # re-prepended fresh in the messages array above.
                max_messages = self._config.llm_max_history_turns * 2
                if len(self._history) > max_messages:
                    self._history = self._history[-max_messages:]
            else:
                # No tokens: immediate error or pre-token cancellation.
                # Pop the user message we added above so history stays in a
                # valid alternating state for the next turn.
                if self._history and self._history[-1]["role"] == "user":
                    self._history.pop()


if __name__ == "__main__":
    cfg = load()
    logging.basicConfig(
        level=getattr(logging, cfg.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    async def _test() -> None:
        agent = LLMAgent(cfg)

        print("\n--- Turn 1 ---")
        async for token in agent.run(
            "What is the speed of light? Give a one sentence answer."
        ):
            # flush=True is required to see tokens arrive incrementally in the
            # terminal. Without it, Python's stdout buffer holds tokens until a
            # newline — making streaming look identical to a blocked response.
            print(token, end="", flush=True)
        print()

        print("\n--- Turn 2 (history check) ---")
        async for token in agent.run("What did I just ask you about?"):
            print(token, end="", flush=True)
        print()

    asyncio.run(_test())
