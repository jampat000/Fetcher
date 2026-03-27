; Inno Setup script (build a friendly installer)
; Requires Inno Setup installed: https://jrsoftware.org/isinfo.php
;
; Install / upgrade / uninstall policy (support):
; - Binaries and WinSW live under {app} (Program Files\Fetcher\Fetcher by default).
; - Live data: SQLite DB, rotating app logs, and migration marker live under
;   %ProgramData%\Fetcher\ (override with machine env FETCHER_DATA_DIR).
; - Upgrade: replaces files under {app}; does NOT remove ProgramData (your DB and logs stay).
; - Uninstall: removes {app} files installed by this script; ProgramData is intentionally NOT
;   deleted here so settings and fetcher.db survive (see README — Logs + install notes).
; - Wrapper WinSW stdout/stderr rolls under {app} — [UninstallDelete] below removes those only.

#define MyAppName "Fetcher"
#define MyAppPublisher "Fetcher"
; Override on command line: ISCC /DMyAppVersion=1.2.3 installer\Fetcher.iss
#ifndef MyAppVersion
#define MyAppVersion "0.0.0-dev"
#endif
#define MyServiceId "Fetcher"

[Setup]
AppId={{F4A8A6E6-0E7A-4E0E-96A6-3B61B30C2B0A}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
DefaultDirName={commonpf}\{#MyAppPublisher}\{#MyAppName}
DefaultGroupName={#MyAppName}
OutputDir=output
OutputBaseFilename=FetcherSetup
Compression=lzma
SolidCompression=yes
PrivilegesRequired=admin
WizardStyle=modern

[UninstallDelete]
; Service wrapper console logs only (not application fetcher.log — that stays under ProgramData\Fetcher\logs).
Type: files; Name: "{app}\*.out.log"
Type: files; Name: "{app}\*.err.log"

[Files]
; Built output (PyInstaller one-folder build + companion one-file exe)
Source: "..\dist\Fetcher\*"; DestDir: "{app}"; Flags: recursesubdirs ignoreversion
; Post-install: run Register-FetcherCompanionTask.ps1 once per user from the Start Menu (interactive session).
Source: "..\scripts\Register-FetcherCompanionTask.ps1"; DestDir: "{app}\scripts"; Flags: ignoreversion
; WinSW + config
Source: "..\service\FetcherService.xml"; DestDir: "{app}"; DestName: "winsw.xml"; Flags: ignoreversion
; WinSW is bundled into the installer (installer/bin/WinSW.exe copied via installer/setup.py)
Source: "..\service\winsw.exe"; DestDir: "{app}"; DestName: "winsw.exe"; Flags: ignoreversion

[Run]
Filename: "{app}\winsw.exe"; Parameters: "install"; Flags: runhidden waituntilterminated
Filename: "{app}\winsw.exe"; Parameters: "start"; Flags: runhidden waituntilterminated
Filename: "http://127.0.0.1:8765"; Description: "Open Fetcher in browser"; Flags: shellexec postinstall nowait skipifsilent

[UninstallRun]
Filename: "{app}\winsw.exe"; Parameters: "stop"; Flags: runhidden waituntilterminated skipifdoesntexist; RunOnceId: "FetcherWinSwStop"
Filename: "{app}\winsw.exe"; Parameters: "uninstall"; Flags: runhidden waituntilterminated skipifdoesntexist; RunOnceId: "FetcherWinSwUninstall"

[Icons]
Name: "{group}\{#MyAppName} (Web UI)"; Filename: "http://127.0.0.1:8765"
Name: "{group}\Register Fetcher Companion (folder picker)"; Filename: "{sys}\WindowsPowerShell\v1.0\powershell.exe"; Parameters: "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File ""{app}\scripts\Register-FetcherCompanionTask.ps1"" -CompanionExe ""{app}\FetcherCompanion.exe"""; Comment: "Run once after install so the folder picker works while Fetcher runs as a service."
Name: "{group}\Uninstall {#MyAppName}"; Filename: "{uninstallexe}"
