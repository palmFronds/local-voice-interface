# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller build spec for openclaw-voice.

Run with:
    pyinstaller build.spec --clean

Output: dist/openclaw-voice/openclaw-voice.exe  (onedir, windowed)

Before distributing, copy your .env file into dist/openclaw-voice/
so the app can find API keys at startup (load_dotenv() searches CWD).
"""

import os
from PyInstaller.utils.hooks import collect_all

# ── Locate native dependencies ────────────────────────────────────────────────
# Using importlib rather than hardcoded paths — the spec executes inside the
# same Python interpreter as the project, so all installed packages are importable.

# _sounddevice_data is a proper Python package whose __init__.py exposes
# __path__, which sounddevice.py uses at runtime to locate portaudio-binaries/.
# collect_all() bundles both the importable package code AND the DLL folder so
# sounddevice can call _sounddevice_data.__path__ correctly inside the frozen app.
_sd_datas, _sd_binaries, _sd_hidden = collect_all('_sounddevice_data')

# deepgram and elevenlabs are FERN-generated SDKs: every subpackage re-exports
# from deeper submodules at import time. Static analysis misses most of the tree,
# so collect_all() is required to ensure every leaf module is included.
_dg_datas, _dg_binaries, _dg_hidden = collect_all('deepgram')
_el_datas, _el_binaries, _el_hidden = collect_all('elevenlabs')

# ── Spec ─────────────────────────────────────────────────────────────────────

a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=_sd_binaries + _dg_binaries + _el_binaries,
    datas=[
        # Bundle the .env.example so the user has a template next to the exe.
        ('.env.example', '.'),
    ] + _sd_datas + _dg_datas + _el_datas,
    hiddenimports=[
        # ── webrtcvad ─────────────────────────────────────────────────────────
        # webrtcvad-wheels ships two files at site-packages root:
        #   webrtcvad.py          — pure Python wrapper (visible to Analysis)
        #   _webrtcvad.<tag>.pyd  — C extension (not inside any package dir)
        # PyInstaller traces the `import _webrtcvad` inside webrtcvad.py but
        # the .pyd sits outside any package dir so we declare it explicitly.
        '_webrtcvad',
        'webrtcvad',

        # ── sounddevice ───────────────────────────────────────────────────────
        # sounddevice.py and _sounddevice.py are standalone .py files (not in a
        # package dir). Analysis finds them, but declaring them ensures inclusion
        # even if the spec is used on a different machine.
        'sounddevice',
        '_sounddevice',
        '_sounddevice_data',

        # ── PyQt6 ─────────────────────────────────────────────────────────────
        # pyinstaller-hooks-contrib has PyQt6 hooks, but declaring the specific
        # submodules used by ui.py prevents them from being tree-shaken away.
        'PyQt6.QtCore',
        'PyQt6.QtWidgets',
        'PyQt6.QtGui',
        'PyQt6.sip',

        # ── websockets ────────────────────────────────────────────────────────
        # websockets 14.x ships a Cython speedup .pyd; the pure-Python fallback
        # is used automatically if the .pyd is absent, but including it keeps
        # performance parity with the development environment.
        'websockets',
        'websockets.speedups',
        'websockets.legacy',
        'websockets.legacy.client',

        # ── cryptography ──────────────────────────────────────────────────────
        # The Rust extension (_rust.pyd) is always loaded; the Ed25519 path is
        # the specific submodule used by agent.py for gateway device-auth signing.
        'cryptography',
        'cryptography.hazmat.primitives.asymmetric.ed25519',
        'cryptography.hazmat.bindings._rust',

        # ── python-dotenv ─────────────────────────────────────────────────────
        # config.py calls load_dotenv() at startup.
        'dotenv',
        'dotenv.main',

        # ── numpy ─────────────────────────────────────────────────────────────
        # audio.py uses numpy for frombuffer() and int16 dtype on playback.
        # sounddevice's InputStream callback also produces numpy arrays.
        'numpy',
        'numpy.core',
    ] + _sd_hidden + _dg_hidden + _el_hidden,
    # hooks/ contains hook-webrtcvad.py which overrides the broken contrib hook
    # that calls copy_metadata('webrtcvad') — incorrect for webrtcvad-wheels.
    hookspath=['hooks'],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # Scientific visualization — not used by this app.
        'matplotlib',
        # Test infrastructure — never imported at runtime.
        'numpy.testing',
        'unittest',
        # tkinter — GUI is PyQt6 only.
        'tkinter',
        # scipy — not used.
        'scipy',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='openclaw-voice',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    # UPX disabled: UPX mishandles the custom PE format of Qt DLLs and can
    # produce an exe that crashes on launch. The size saving (~30%) is not
    # worth the reliability risk on Windows.
    upx=False,
    # console=False: no terminal window. All output goes to a log file if
    # the user redirects, or is silent. Matches the GUI-only design of ui.py.
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name='openclaw-voice',
)
