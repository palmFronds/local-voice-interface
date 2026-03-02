"""Configuration for Simple Voice Interface.

Single source of truth for all constants and tunables.
All values are loaded from environment variables by load().
Magic numbers live in this file only — nowhere else in the codebase.
"""

from __future__ import annotations

import dataclasses
import logging
import os

from dotenv import load_dotenv


@dataclasses.dataclass(frozen=True)
class Config:
    """Immutable configuration for the Simple Voice Interface pipeline.

    Required fields (no default) must be set in .env — load() will raise
    ValueError with the missing variable name if they are absent.

    chunk_size is always derived from sample_rate and chunk_ms inside load().
    It is never read from the environment directly.

    All default values defined here are the single canonical source of truth.
    load() references these defaults via dataclasses.fields() so the values
    are never duplicated.
    """

    # ── Required: API keys — no defaults, must be present in .env ────────────
    deepgram_api_key: str
    tts_api_key: str
    tts_voice_id: str

    # ── Derived: computed from sample_rate × chunk_ms, never read from env ───
    chunk_size: int  # = sample_rate * chunk_ms // 1000  (e.g. 320 samples at 16 kHz / 20 ms)

    # ── Audio ─────────────────────────────────────────────────────────────────
    sample_rate: int = 16_000     # Hz — 16 kHz required by webrtcvad
    chunk_ms: int = 20            # ms per frame — 10/20/30 ms are the only values webrtcvad accepts

    # ── Voice Activity Detection ──────────────────────────────────────────────
    vad_aggressiveness: int = 2   # 0 (least) – 3 (most); 2 balances sensitivity vs. false positives
    silence_duration_ms: int = 600  # ms of continuous silence that ends an utterance
    vad_energy_threshold: float = 300.0  # RMS gate applied before webrtcvad; filters quiet hiss

    # ── Speech-to-Text ────────────────────────────────────────────────────────
    deepgram_model: str = "nova-2"
    deepgram_endpointing_ms: int = 300  # ms of silence before Deepgram finalizes a transcript

    # ── Text-to-Speech ────────────────────────────────────────────────────────
    tts_provider: str = "elevenlabs"  # "elevenlabs" or "cartesia"
    tts_timeout: float = 5.0          # seconds to wait for first audio chunk before giving up

    # ── LLM Agent (Phase 1) ───────────────────────────────────────────────────
    llm_api_base: str = "https://api.openai.com/v1"
    llm_api_key: str = ""  # Optional in Phase 2 — OpenClaw owns the LLM connection
    llm_model: str = "gpt-4o-mini"
    llm_system_prompt: str = (
        "You are a helpful voice assistant. Be concise. Your responses will be spoken aloud."
    )
    llm_max_history_turns: int = 20  # Rolling window; system prompt is always preserved

    # ── OpenClaw Gateway (Phase 2) ────────────────────────────────────────────
    llm_ws_url: str = "ws://127.0.0.1:18789"  # Gateway WebSocket URL
    openclaw_gateway_token: str = ""           # Auth token from ~/.openclaw/openclaw.json → gateway.auth.token
    openclaw_cmd: str = "openclaw"             # Binary name — kept for reference; not used by WS approach

    # ── Logging ───────────────────────────────────────────────────────────────
    log_level: str = "INFO"


def load() -> Config:
    """Load configuration from .env and the process environment.

    Reads .env via python-dotenv (silently skips if not found), then reads
    each variable from os.environ.  Optional variables fall back to the
    defaults declared on Config fields, keeping magic numbers in one place.
    chunk_size is computed from sample_rate and chunk_ms — it is never read
    from the environment.

    Returns:
        A frozen Config instance with all values populated.

    Raises:
        ValueError: If any required environment variable is absent or empty.
    """
    load_dotenv()

    # For distribution builds, set keys via environment or .env before running
    # pyinstaller build.spec — PyInstaller bakes whatever is in the environment
    # at build time into the frozen bytecode. Keys never need to appear in source.

    # Build a lookup of field defaults declared on Config so we never
    # repeat the same numbers here — Config is the single source of truth.
    field_defaults: dict[str, object] = {
        f.name: f.default
        for f in dataclasses.fields(Config)
        if f.default is not dataclasses.MISSING
    }

    def _require(env_key: str) -> str:
        """Return the value of a required env var, or raise with a clear message."""
        value = os.getenv(env_key)
        if not value:
            raise ValueError(
                f"Required environment variable '{env_key}' is missing or empty. "
                f"Copy .env.example to .env and fill in all required values."
            )
        return value

    def _optional(env_key: str, field_name: str) -> str:
        """Return env var value, falling back to the Config field default."""
        return os.getenv(env_key, str(field_defaults[field_name]))

    def _optional_int(env_key: str, field_name: str) -> int:
        return int(_optional(env_key, field_name))

    def _optional_float(env_key: str, field_name: str) -> float:
        return float(_optional(env_key, field_name))

    # Resolve audio settings first so we can derive chunk_size from them.
    sample_rate = _optional_int("SAMPLE_RATE", "sample_rate")
    chunk_ms = _optional_int("CHUNK_MS", "chunk_ms")

    # Derived — webrtcvad requires exactly (sample_rate / 1000 * chunk_ms) samples per frame.
    chunk_size = sample_rate * chunk_ms // 1000

    return Config(
        # Required API keys
        deepgram_api_key=_require("DEEPGRAM_API_KEY"),
        tts_api_key=_require("TTS_API_KEY"),
        tts_voice_id=_require("TTS_VOICE_ID"),
        # Derived
        chunk_size=chunk_size,
        # Audio
        sample_rate=sample_rate,
        chunk_ms=chunk_ms,
        # VAD
        vad_aggressiveness=_optional_int("VAD_AGGRESSIVENESS", "vad_aggressiveness"),
        silence_duration_ms=_optional_int("SILENCE_DURATION_MS", "silence_duration_ms"),
        vad_energy_threshold=_optional_float("VAD_ENERGY_THRESHOLD", "vad_energy_threshold"),
        # STT
        deepgram_model=_optional("DEEPGRAM_MODEL", "deepgram_model"),
        deepgram_endpointing_ms=_optional_int("DEEPGRAM_ENDPOINTING_MS", "deepgram_endpointing_ms"),
        # TTS
        tts_provider=_optional("TTS_PROVIDER", "tts_provider"),
        tts_timeout=_optional_float("TTS_TIMEOUT", "tts_timeout"),
        # LLM (Phase 1 only — llm_api_key is optional in Phase 2)
        llm_api_base=_optional("LLM_API_BASE", "llm_api_base"),
        llm_api_key=_optional("LLM_API_KEY", "llm_api_key"),
        llm_model=_optional("LLM_MODEL", "llm_model"),
        llm_system_prompt=_optional("LLM_SYSTEM_PROMPT", "llm_system_prompt"),
        llm_max_history_turns=_optional_int("LLM_MAX_HISTORY_TURNS", "llm_max_history_turns"),
        # OpenClaw Gateway (Phase 2)
        llm_ws_url=_optional("LLM_WS_URL", "llm_ws_url"),
        openclaw_gateway_token=_optional("OPENCLAW_GATEWAY_TOKEN", "openclaw_gateway_token"),
        openclaw_cmd=_optional("OPENCLAW_CMD", "openclaw_cmd"),
        # Logging
        log_level=_optional("LOG_LEVEL", "log_level"),
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    config = load()
    print(config)
