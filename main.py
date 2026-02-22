import asyncio
import logging

from config import load
from audio import AudioController
from stt import StreamingSTT
from agent import LLMAgent
from tts import StreamingTTS
from state import VoiceStateMachine

async def main() -> None:
    """Entry point. Instantiates all components and starts the state machine.
    Contains no business logic — all behaviour lives in the component files.
    """
    config = load()
    logging.basicConfig(
        level=getattr(logging, config.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger(__name__).info("Simple Voice Interface starting")

    audio_controller = AudioController(config)
    stt_engine = StreamingSTT(config)
    llm_agent = LLMAgent(config)
    tts_engine = StreamingTTS(config)

    machine = VoiceStateMachine(
        audio=audio_controller,
        stt=stt_engine,
        agent=llm_agent,
        tts=tts_engine,
        config=config,
    )
    await machine.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Goodbye.")
