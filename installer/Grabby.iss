; Inno Setup script (build a friendly installer)
; Requires Inno Setup installed: https://jrsoftware.org/isinfo.php

#define MyAppName "Grabby"
#define MyAppPublisher "Grabby"
#define MyAppVersion "0.1.0"
#define MyServiceId "Grabby"

[Setup]
AppId={{F4A8A6E6-0E7A-4E0E-96A6-3B61B30C2B0A}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
DefaultDirName={commonpf}\{#MyAppPublisher}\Grabby
DefaultGroupName={#MyAppName}
OutputDir=output
OutputBaseFilename=GrabbySetup
Compression=lzma
SolidCompression=yes
PrivilegesRequired=admin
WizardStyle=modern

[Files]
; Built output (PyInstaller one-folder build)
Source: "..\dist\Grabby\*"; DestDir: "{app}"; Flags: recursesubdirs ignoreversion
; WinSW + config
Source: "..\service\GrabbyService.xml"; DestDir: "{app}"; DestName: "winsw.xml"; Flags: ignoreversion
; WinSW is bundled into the installer (installer/build.ps1 downloads it automatically)
Source: "..\service\winsw.exe"; DestDir: "{app}"; DestName: "winsw.exe"; Flags: ignoreversion

[Run]
Filename: "{app}\winsw.exe"; Parameters: "install"; Flags: runhidden waituntilterminated
Filename: "{app}\winsw.exe"; Parameters: "start"; Flags: runhidden waituntilterminated
Filename: "http://127.0.0.1:8765"; Description: "Open Grabby in browser"; Flags: shellexec postinstall nowait skipifsilent

[UninstallRun]
Filename: "{app}\winsw.exe"; Parameters: "stop"; Flags: runhidden waituntilterminated skipifdoesntexist
Filename: "{app}\winsw.exe"; Parameters: "uninstall"; Flags: runhidden waituntilterminated skipifdoesntexist

[Icons]
Name: "{group}\{#MyAppName} (Web UI)"; Filename: "http://127.0.0.1:8765"
Name: "{group}\Uninstall {#MyAppName}"; Filename: "{uninstallexe}"
