"""Streaming TTS for Simple Voice Interface.

Buffers incoming text tokens to phrase boundaries, streams each phrase to
ElevenLabs via the streaming API, and yields raw PCM audio chunks as they
arrive — so playback can begin before the full LLM response is complete.
"""

from __future__ import annotations

import asyncio
import logging
from typing import AsyncIterator

from elevenlabs import AsyncElevenLabs

from config import Config, load

logger = logging.getLogger(__name__)

# ElevenLabs model for low-latency streaming TTS.
_TTS_MODEL = "eleven_turbo_v2_5"

# Characters that mark natural phrase boundaries. Flushing at these avoids
# sending individual words or partial sentences, which produce choppy prosody
# because ElevenLabs optimises intonation within the text it receives in one
# request. Longer phrases → better-sounding speech.
_FLUSH_PUNCTUATION = frozenset(".!?,;:")

# Number of tokens to accumulate before flushing even without punctuation.
# Prevents single-word requests when the LLM generates long unpunctuated runs.
_MIN_BUFFER_TOKENS = 5


class StreamingTTS:
    """Converts a streaming text token iterator to a streaming PCM audio iterator.

    Derived from Vocalis's tts.py stream_text_to_speech() (which batches the
    full text into a single blocking HTTP call to a local API). Differs by:
    - Receiving text as an async token stream (not a pre-built string)
    - Flushing to ElevenLabs in phrase chunks as they form
    - Using AsyncElevenLabs SDK with output_format='pcm_16000' for native PCM
    - Yielding audio as it arrives rather than after the full response completes

    The PCM output format eliminates the mp3 → PCM conversion step that a raw
    HTTP approach would require (Vocalis's local API returns WAV; ElevenLabs
    returns mp3 by default — native pcm_16000 avoids the pydub/ffmpeg path).
    """

    def __init__(self, config: Config) -> None:
        """Initialise StreamingTTS.

        Args:
            config: Frozen Config instance from config.load().
        """
        self._config = config
        # AsyncElevenLabs is stateless at construction — only credentials stored.
        # Creating once validates the API key format before the first synthesize() call.
        self._client = AsyncElevenLabs(api_key=config.tts_api_key)

    async def _stream_phrase(self, text: str) -> AsyncIterator[bytes]:
        """Send one buffered phrase to ElevenLabs and yield raw PCM audio chunks.

        Applies asyncio.wait_for() to the first chunk only. The first chunk is
        when the HTTP connection is established and ElevenLabs begins synthesis.
        Subsequent chunks arrive quickly once streaming starts, so applying the
        timeout there would incorrectly abort long responses.

        Args:
            text: A phrase-sized text string to synthesise.

        Yields:
            Raw PCM bytes chunks (int16, mono, 16 kHz).
        """
        text = text.strip()
        if not text:
            return
        logger.debug("TTS phrase: %r", text[:80])
        # stream() returns an AsyncIterator directly — it is NOT a coroutine.
        # Do not await it; iterate it with async for.
        audio_stream = self._client.text_to_speech.stream(
            voice_id=self._config.tts_voice_id,
            text=text,
            output_format="pcm_16000",  # int16 mono 16 kHz — matches sounddevice config
            model_id=_TTS_MODEL,
        )
        aiter = audio_stream.__aiter__()

        # Wait for the first chunk with a hard timeout. If ElevenLabs does not
        # respond in time (network hang, rate limit, API down), return without
        # yielding. The caller (synthesize) produces no audio for this phrase;
        # tts_done_event is still set by _run_tts so the FSM transitions cleanly.
        try:
            first_chunk = await asyncio.wait_for(
                aiter.__anext__(), timeout=self._config.tts_timeout
            )
        except StopAsyncIteration:
            return  # Empty response from ElevenLabs — nothing to yield.
        except asyncio.TimeoutError:
            logger.warning(
                "TTS timeout after %.1fs — ElevenLabs did not respond", self._config.tts_timeout
            )
            return

        if first_chunk:
            yield first_chunk

        # Remaining chunks arrive quickly once the stream is open; no timeout needed.
        async for chunk in aiter:
            if chunk:  # SDK may emit empty bytes between real chunks
                yield chunk

    async def synthesize(
        self,
        token_stream: AsyncIterator[str],
    ) -> AsyncIterator[bytes]:
        """Consume text tokens; yield raw PCM audio chunk bytes.

        Buffers tokens to phrase boundaries before sending to ElevenLabs. This
        produces coherent prosody — flushing word-by-word would reset ElevenLabs's
        intonation model on every request, resulting in flat, robotic output.
        Audio chunks are yielded as they arrive so playback begins before the
        LLM has finished producing the full response.

        Args:
            token_stream: Async iterator of text tokens from LLMAgent.run().

        Yields:
            Raw PCM bytes chunks (int16, mono, 16 kHz) ready for sounddevice.
        """
        buffer: list[str] = []

        try:
            async for token in token_stream:
                buffer.append(token)
                joined = "".join(buffer)
                # Flush on punctuation or when the buffer reaches minimum size.
                # Check the rightmost non-whitespace character for punctuation —
                # tokens often trail with a space (e.g. "word "), so rstrip first.
                stripped = joined.rstrip()
                should_flush = bool(stripped) and (
                    stripped[-1] in _FLUSH_PUNCTUATION
                    or len(buffer) >= _MIN_BUFFER_TOKENS
                )
                if should_flush:
                    async for chunk in self._stream_phrase(joined):
                        yield chunk
                    buffer.clear()

            # Flush any tokens remaining after the token stream closes.
            if buffer:
                async for chunk in self._stream_phrase("".join(buffer)):
                    yield chunk

        except asyncio.CancelledError:
            logger.info("TTS synthesize cancelled")
            raise
        except Exception as exc:
            logger.warning("TTS error: %s", exc)


if __name__ == "__main__":
    import numpy as np
    import sounddevice as sd

    cfg = load()
    logging.basicConfig(
        level=getattr(logging, cfg.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    # Sentence split into tokens to simulate an LLM stream arriving token-by-token.
    # flush=True equivalent: each print call is visible immediately as tokens arrive.
    _TEST_TOKENS = [
        "The", " speed", " of", " light", " in", " a", " vacuum",
        " is", " approximately", " 299", ",", "792", ",", "458",
        " metres", " per", " second", ".",
    ]

    async def _test() -> None:
        tts = StreamingTTS(cfg)

        async def _token_source() -> AsyncIterator[str]:
            for tok in _TEST_TOKENS:
                yield tok
                await asyncio.sleep(0.05)  # Simulate LLM token arrival rate

        logger.info("Synthesizing test sentence (%d tokens)...", len(_TEST_TOKENS))
        pcm_chunks: list[bytes] = []

        async for chunk in tts.synthesize(_token_source()):
            pcm_chunks.append(chunk)
            logger.debug("Audio chunk received: %d bytes", len(chunk))

        if not pcm_chunks:
            logger.error("No audio received — check TTS_API_KEY and TTS_VOICE_ID in .env")
            return

        all_pcm = b"".join(pcm_chunks)
        duration_s = len(all_pcm) / (cfg.sample_rate * 2)  # int16 = 2 bytes per sample
        logger.info("Received %.1fs of audio (%d bytes total)", duration_s, len(all_pcm))

        audio_array = np.frombuffer(all_pcm, dtype=np.int16)
        logger.info("Playing audio...")
        sd.play(audio_array, samplerate=cfg.sample_rate)
        sd.wait()
        logger.info("Playback complete")

    asyncio.run(_test())
