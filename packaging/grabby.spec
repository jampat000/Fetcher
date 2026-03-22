import os

from PyInstaller.utils.hooks import collect_submodules

# PyInstaller spec for Windows single-folder build.
# Usage:
#   py -m pip install pyinstaller
#   py -m PyInstaller packaging/grabby.spec

block_cipher = None

# PyInstaller defines SPECPATH while executing this file.
ROOT = os.path.abspath(os.path.join(SPECPATH, ".."))

_version_file = os.path.join(ROOT, "VERSION")
_extra_datas = []
if os.path.isfile(_version_file):
    _extra_datas.append((_version_file, "."))

hiddenimports = []
hiddenimports += collect_submodules("apscheduler")
hiddenimports += collect_submodules("sqlalchemy")
hiddenimports += collect_submodules("aiosqlite")
hiddenimports += ["yaml", "services", "services.api_keys"]

a = Analysis(
    [os.path.join(ROOT, "app", "cli.py")],
    pathex=[ROOT],
    binaries=[],
    datas=[
        (os.path.join(ROOT, "app", "templates"), "app/templates"),
        (os.path.join(ROOT, "app", "static"), "app/static"),
        *_extra_datas,
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
    name="Grabby",
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
    name="Grabby",
)
