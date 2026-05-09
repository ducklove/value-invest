param(
    [Parameter(Mandatory = $true)]
    [string]$Remote,

    [string]$RemoteDbPath = "/home/pi/value-invest/cache.db",
    [string]$LocalDbPath = "",
    [string]$SourceEmail = "",
    [string]$SourceGoogleSub = "",
    [string]$DestGoogleSub = "",
    [string]$DestEmail = "",
    [string]$DestName = "",
    [switch]$NoBackup
)

$ErrorActionPreference = "Stop"
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
if (-not $LocalDbPath) {
    $LocalDbPath = Join-Path $repoRoot "cache.db"
}

$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$stageDir = Join-Path $repoRoot "data\db-imports"
New-Item -ItemType Directory -Force -Path $stageDir | Out-Null

$remoteBackup = "/tmp/value-invest-cache-$stamp.db"
$localSnapshot = Join-Path $stageDir "prod-cache-$stamp.db"

$remotePy = "import sqlite3; src=sqlite3.connect('$RemoteDbPath'); dst=sqlite3.connect('$remoteBackup'); src.backup(dst); dst.close(); src.close()"

Write-Host "Creating consistent SQLite backup on $Remote..."
ssh $Remote "python3 -c `"$remotePy`""

try {
    Write-Host "Downloading $remoteBackup to $localSnapshot..."
    scp "${Remote}:$remoteBackup" $localSnapshot
}
finally {
    Write-Host "Cleaning remote temporary backup..."
    ssh $Remote "rm -f '$remoteBackup'" | Out-Null
}

$argsList = @(
    (Join-Path $repoRoot "scripts\import_portfolio_db.py"),
    "--source", $localSnapshot,
    "--target", $LocalDbPath
)
if ($SourceEmail) { $argsList += @("--source-email", $SourceEmail) }
if ($SourceGoogleSub) { $argsList += @("--source-google-sub", $SourceGoogleSub) }
if ($DestGoogleSub) { $argsList += @("--dest-google-sub", $DestGoogleSub) }
if ($DestEmail) { $argsList += @("--dest-email", $DestEmail) }
if ($DestName) { $argsList += @("--dest-name", $DestName) }
if ($NoBackup) { $argsList += "--no-backup" }

Write-Host "Importing portfolio rows into $LocalDbPath..."
python @argsList
