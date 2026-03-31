; Inno Setup script (build a friendly installer)
; Requires Inno Setup installed: https://jrsoftware.org/isinfo.php
;
; Install / upgrade / uninstall policy (support):
; - Binaries and WinSW live under {app} (Program Files\Fetcher\Fetcher by default).
; - Live data: SQLite DB and rotating app logs under %ProgramData%\Fetcher\
;   (override with machine env FETCHER_DATA_DIR).
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
CloseApplications=yes

[Code]

procedure StopFetcherServiceBeforeFileCopy;
var
  ResultCode: Integer;
  WinswPath: String;
begin
  { Avoid locked Fetcher.exe / DLLs during upgrade: stop the service before [Files] replaces binaries. }
  WinswPath := ExpandConstant('{app}\winsw.exe');
  if FileExists(WinswPath) then
  begin
    Exec(WinswPath, 'stop', ExpandConstant('{app}'), SW_HIDE, ewWaitUntilTerminated, ResultCode);
    Sleep(3000);
  end;
end;

function FetcherServiceRegistered: Boolean;
var
  ResultCode: Integer;
  ScExe: String;
begin
  { WinSW <id>Fetcher</id> → Windows service name Fetcher. Do not guess from winsw restart exit codes:
    restart can succeed (exit 0) when the service was never installed, which skipped install+start. }
  ScExe := ExpandConstant('{sys}\sc.exe');
  Result := FileExists(ScExe) and
    Exec(ScExe, 'query Fetcher', '', SW_HIDE, ewWaitUntilTerminated, ResultCode) and
    (ResultCode = 0);
end;

procedure RegisterWinSwServiceAfterInstall;
var
  ResultCode: Integer;
  AppDir: String;
  WinswPath: String;
  ScExe: String;
begin
  AppDir := ExpandConstant('{app}');
  WinswPath := AppDir + '\winsw.exe';
  ScExe := ExpandConstant('{sys}\sc.exe');
  if not FileExists(WinswPath) then
    Exit;
  if FetcherServiceRegistered then
  begin
    Exec(WinswPath, 'restart', AppDir, SW_HIDE, ewWaitUntilTerminated, ResultCode);
    if ResultCode <> 0 then
      Exec(WinswPath, 'start', AppDir, SW_HIDE, ewWaitUntilTerminated, ResultCode);
    if ResultCode <> 0 then
      Exec(ScExe, 'start Fetcher', '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
  end
  else
  begin
    Exec(WinswPath, 'install', AppDir, SW_HIDE, ewWaitUntilTerminated, ResultCode);
    Exec(WinswPath, 'start', AppDir, SW_HIDE, ewWaitUntilTerminated, ResultCode);
    if ResultCode <> 0 then
      Exec(ScExe, 'start Fetcher', '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
  end;
end;

procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep = ssInstall then
    StopFetcherServiceBeforeFileCopy;
  if CurStep = ssPostInstall then
    RegisterWinSwServiceAfterInstall;
end;

[InstallDelete]
; Browse/companion-era files from pre-3.1.0 installs — removed before new files copy (upgrade path).
Type: files; Name: "{app}\FetcherCompanion.exe"
Type: files; Name: "{app}\Install-FetcherCompanionTask.ps1"
Type: files; Name: "{app}\Register-FetcherCompanionTask.ps1"

[UninstallDelete]
; Service wrapper console logs only (not application fetcher.log — that stays under ProgramData\Fetcher\logs).
Type: files; Name: "{app}\*.out.log"
Type: files; Name: "{app}\*.err.log"
; Same companion-era filenames if still present at uninstall (e.g. upgraded from 3.0.x).
Type: files; Name: "{app}\FetcherCompanion.exe"
Type: files; Name: "{app}\Install-FetcherCompanionTask.ps1"
Type: files; Name: "{app}\Register-FetcherCompanionTask.ps1"

[Files]
; Built output (PyInstaller one-folder build)
Source: "..\dist\Fetcher\*"; DestDir: "{app}"; Flags: recursesubdirs ignoreversion
; WinSW + config
Source: "..\service\FetcherService.xml"; DestDir: "{app}"; DestName: "winsw.xml"; Flags: ignoreversion
; WinSW is bundled into the installer (installer/bin/WinSW.exe copied via installer/setup.py)
Source: "..\service\winsw.exe"; DestDir: "{app}"; DestName: "winsw.exe"; Flags: ignoreversion

[Run]
; WinSW install/start/restart runs from [Code] CurStepChanged(ssPostInstall) so upgrades can use restart.
Filename: "http://127.0.0.1:8765"; Description: "Open Fetcher in browser"; Flags: shellexec postinstall nowait skipifsilent

[UninstallRun]
Filename: "{app}\winsw.exe"; Parameters: "stop"; Flags: runhidden waituntilterminated skipifdoesntexist; RunOnceId: "FetcherWinSwStop"
Filename: "{app}\winsw.exe"; Parameters: "uninstall"; Flags: runhidden waituntilterminated skipifdoesntexist; RunOnceId: "FetcherWinSwUninstall"

[Icons]
Name: "{group}\{#MyAppName} (Web UI)"; Filename: "http://127.0.0.1:8765"
Name: "{group}\Uninstall {#MyAppName}"; Filename: "{uninstallexe}"
