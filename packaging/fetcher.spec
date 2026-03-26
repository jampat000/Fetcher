import os

from PyInstaller.utils.hooks import collect_all, collect_submodules

# PyInstaller spec for Windows single-folder build.
# Usage:
#   py -m pip install pyinstaller
#   py -m PyInstaller packaging/fetcher.spec

block_cipher = None

# PyInstaller defines SPECPATH while executing this file.
ROOT = os.path.abspath(os.path.join(SPECPATH, ".."))
# Bundle contents come only from explicit ``datas`` below (templates, static, VERSION) plus traced
# imports from ``app/cli.py``. Repo tooling (e.g. ``.cursor/``) is not packaged.

_version_file = os.path.join(ROOT, "VERSION")
_extra_datas = []
if os.path.isfile(_version_file):
    _extra_datas.append((_version_file, "."))
_ffmpeg_stage = os.path.join(ROOT, "packaging", "ffmpeg-bin")
_ffmpeg_exe = os.path.join(_ffmpeg_stage, "ffmpeg.exe")
_ffprobe_exe = os.path.join(_ffmpeg_stage, "ffprobe.exe")
if os.path.isfile(_ffmpeg_exe) and os.path.isfile(_ffprobe_exe):
    _extra_datas.append((_ffmpeg_exe, os.path.join("bin", "ffmpeg")))
    _extra_datas.append((_ffprobe_exe, os.path.join("bin", "ffmpeg")))

hiddenimports = []
hiddenimports += collect_submodules("apscheduler")
hiddenimports += collect_submodules("sqlalchemy")
hiddenimports += collect_submodules("aiosqlite")
hiddenimports += collect_submodules("passlib")
hiddenimports += collect_submodules("slowapi")
hiddenimports += collect_submodules("limits")
hiddenimports += ["yaml", "app.resolvers", "app.resolvers.api_keys", "bcrypt", "itsdangerous", "_cffi_backend"]

# ASGI server + uvicorn[standard] extras — PyInstaller often misses these (dynamic imports),
# which breaks the frozen exe at `import uvicorn` (see CI smoke test /healthz).
_uvicorn_datas, _uvicorn_binaries, _uvicorn_hidden = collect_all("uvicorn")
hiddenimports += _uvicorn_hidden
# uvicorn[standard] native / lazy-loaded deps (CI one-folder exe must not fail at import time).
hiddenimports += collect_submodules("httptools")
hiddenimports += collect_submodules("websockets")
hiddenimports += collect_submodules("watchfiles")

a = Analysis(
    [os.path.join(ROOT, "app", "cli.py")],
    pathex=[ROOT],
    binaries=_uvicorn_binaries,
    datas=[
        (os.path.join(ROOT, "app", "templates"), "app/templates"),
        (os.path.join(ROOT, "app", "static"), "app/static"),
        *_extra_datas,
        *_uvicorn_datas,
    ],
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="Fetcher",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="Fetcher",
)
