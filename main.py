import asyncio
import logging
import sys
import threading

from PyQt6.QtWidgets import QApplication

from config import load
from audio import AudioController
from stt import StreamingSTT
from agent import LLMAgent
from tts import StreamingTTS
from state import VoiceStateMachine
from ui import OrbWidget, ui_state_queue


def run_pipeline() -> None:
    """Run the asyncio voice pipeline on a background thread.

    asyncio.run() creates a fresh event loop for this thread so the pipeline
    never shares a loop with the Qt main thread. daemon=True means this thread
    dies automatically when the Qt window closes without needing explicit cleanup.
    """
    async def _main() -> None:
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

    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    app = QApplication(sys.argv)
    orb = OrbWidget()
    orb.show()
    # Set orb to inactive immediately so it's visible before the pipeline boots.
    ui_state_queue.put("inactive")

    pipeline_thread = threading.Thread(target=run_pipeline, daemon=True)
    pipeline_thread.start()

    sys.exit(app.exec())
