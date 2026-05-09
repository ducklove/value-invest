param(
    [int]$Port = 8000,
    [string]$HostAddress = "127.0.0.1"
)

$ErrorActionPreference = "Stop"
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $repoRoot

if (-not $env:VALUE_INVEST_ENV) {
    $env:VALUE_INVEST_ENV = "development"
}

python -m uvicorn main:app --reload --host $HostAddress --port $Port

