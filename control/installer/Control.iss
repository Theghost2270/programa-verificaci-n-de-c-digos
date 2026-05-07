; Build this installer with Inno Setup Compiler (ISCC.exe)
; Output app installer in installer\output\

#define MyAppName "Control"
#define MyAppVersion "0.1.0"
#define MyAppPublisher "Control"
#define MyAppExeName "control.exe"
#define MyAppDbExeName "control-db.exe"

[Setup]
AppId={{2D9DFB2C-0A90-4F7E-8595-EB5605A16A81}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
DefaultDirName={autopf}\{#MyAppName}
DefaultGroupName={#MyAppName}
DisableProgramGroupPage=yes
OutputDir=output
OutputBaseFilename=Control-Setup
Compression=lzma
SolidCompression=yes
WizardStyle=modern
ArchitecturesInstallIn64BitMode=x64

[Languages]
Name: "spanish"; MessagesFile: "compiler:Languages\Spanish.isl"

[Tasks]
Name: "desktopicon"; Description: "Crear acceso directo en el escritorio"; GroupDescription: "Accesos directos:"

[Files]
Source: "..\dist\{#MyAppExeName}"; DestDir: "{app}"; Flags: ignoreversion
Source: "..\dist\{#MyAppDbExeName}"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\Control"; Filename: "{app}\{#MyAppExeName}"
Name: "{group}\Control DB Viewer"; Filename: "{app}\{#MyAppDbExeName}"
Name: "{autodesktop}\Control"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "Abrir Control"; Flags: nowait postinstall skipifsilent
