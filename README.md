# OpenClaw Voice

OpenClaw Voice is a hands-free voice interface for your OpenClaw agent. Speak naturally into your microphone, hear your agent respond through your speakers, and interrupt it mid-sentence at any time — it stops immediately and starts listening again. Everything runs locally on your machine except the speech recognition and voice synthesis, which connect to cloud APIs that are already configured in the application.

## Requirements

- Windows 10 or 11 (64-bit)
- OpenClaw installed and available on your system PATH
- An OpenClaw agent configured and ready to respond

## Running It

1. Unzip the `openclaw-voice` folder to any location.
2. Double-click `openclaw-voice.exe`.
3. The orb appears in the centre of your screen — you're live.

To quit, right-click anywhere on the orb window.

## The Orb

The orb is your only interface. Its colour tells you exactly what the system is doing.

**Grey** — Starting up or unavailable. The application is either still connecting to the OpenClaw gateway or could not reach it. If the orb stays grey, see Troubleshooting.

**Blue** — Listening. The microphone is open and the system is waiting for you to speak.

**Violet** — Thinking. Your words have been transcribed and sent to the agent. It is generating a response.

**Emerald** — Speaking. The agent is talking. You can interrupt at any time by speaking — it stops within a fraction of a second and the orb returns to blue.

## How It Works

When you speak, your microphone audio is streamed continuously to a speech recognition service, which transcribes your words and detects when you have finished a thought. That transcript goes to your OpenClaw agent, which begins generating a response immediately. The first word of the response reaches your speakers within about a second — the system does not wait for the full answer to be written before it starts speaking.

While the agent is talking, a voice activity detector monitors your microphone. If you speak for more than a fraction of a second, the system cancels the agent mid-sentence, discards any audio still queued for playback, and returns to listening. Nothing is replayed. The entire pipeline runs as a single local process on your machine. The only outbound network calls are to the speech-to-text and text-to-speech services.

## Troubleshooting

**Orb stays grey.** The application could not start or connect to the OpenClaw gateway. Confirm that OpenClaw is installed by opening a terminal and running `openclaw`. If you installed it via npm, ensure your npm global bin folder is on your system PATH and try restarting the application after opening a fresh terminal session.

**No audio output.** Playback goes through your Windows default audio device. Open Sound settings, confirm the correct speaker or headphones are set as the default playback device, and check that the volume is not muted.

**Speech is not recognised.** Speak clearly and at a normal pace. Open Sound settings and verify that your intended microphone is set as the default recording device. If you have multiple microphones connected, Windows may be capturing from the wrong one.

**Application crashes on launch.** The application produces diagnostic output as it starts. If an error window appears, copy its full contents and send them to the developer for investigation.

## Building from Source

This section is for developers only.

Prerequisites: Python 3.11 or later, all packages listed in `requirements.txt`, and OpenClaw installed. Copy `.env.example` to `.env` and fill in your Deepgram API key, ElevenLabs API key, voice ID, and OpenClaw gateway token before building.

1. Fill in `.env` with all required API keys.
2. Run `pyinstaller build.spec --clean`.
3. The distributable output lands in `dist/openclaw-voice/`. Zip that folder for distribution.

Never commit `.env` or the `_BAKED` credentials block in `config.py` to version control. Both contain live API keys.
