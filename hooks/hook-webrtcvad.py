# Custom hook to override the broken pyinstaller-hooks-contrib hook-webrtcvad.py.
#
# pyinstaller-hooks-contrib ships a hook that calls copy_metadata('webrtcvad'),
# but this project uses the webrtcvad-wheels drop-in which registers its dist-info
# under the name 'webrtcvad-wheels', not 'webrtcvad'. copy_metadata('webrtcvad')
# raises PackageNotFoundError and aborts the build.
#
# This replacement collects metadata from the correct dist name and explicitly
# declares the C extension (_webrtcvad) as a hidden import so PyInstaller includes
# the .pyd even though webrtcvad.py imports it with a name that lacks a package prefix.

from PyInstaller.utils.hooks import copy_metadata

datas = copy_metadata('webrtcvad-wheels')
hiddenimports = ['_webrtcvad']
