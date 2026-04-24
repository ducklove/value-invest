param(
    [string]$Root = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path,
    [switch]$PullClean
)

$ErrorActionPreference = "Stop"

$projects = @(
    [pscustomobject]@{
        Name = "holding_value"
        Repo = "https://github.com/ducklove/holding_value.git"
        ExistingDirs = @("hodling-value", "holding_value")
        CloneDir = "holding_value"
    },
    [pscustomobject]@{
        Name = "common_preferred_spread"
        Repo = "https://github.com/ducklove/common_preferred_spread.git"
        ExistingDirs = @("common_preferred_spread")
        CloneDir = "common_preferred_spread"
    },
    [pscustomobject]@{
        Name = "kis-proxy"
        Repo = "https://github.com/ducklove/kis-proxy.git"
        ExistingDirs = @("kis-proxy")
        CloneDir = "kis-proxy"
    },
    [pscustomobject]@{
        Name = "gold_gap"
        Repo = "https://github.com/ducklove/gold_gap.git"
        ExistingDirs = @("gold_gap")
        CloneDir = "gold_gap"
    }
)

function Resolve-ProjectPath($Project) {
    foreach ($dir in $Project.ExistingDirs) {
        $candidate = Join-Path $Root $dir
        if (Test-Path -LiteralPath $candidate) {
            return $candidate
        }
    }
    return Join-Path $Root $Project.CloneDir
}

foreach ($project in $projects) {
    Write-Host ""
    Write-Host "== $($project.Name) =="

    $path = Resolve-ProjectPath $project
    if (-not (Test-Path -LiteralPath $path)) {
        Write-Host "Cloning $($project.Repo) -> $path"
        git clone $project.Repo $path
        continue
    }

    if (-not (Test-Path -LiteralPath (Join-Path $path ".git"))) {
        Write-Warning "Skipping $path because it is not a git repository."
        continue
    }

    git -C $path fetch --prune

    $branch = git -C $path branch --show-current
    $status = @(git -C $path status --short)
    $upstream = git -C $path rev-parse --abbrev-ref --symbolic-full-name "@{u}" 2>$null
    if ($LASTEXITCODE -ne 0) {
        $upstream = ""
    }

    Write-Host "Path: $path"
    Write-Host "Branch: $branch"
    if ($upstream) {
        Write-Host "Upstream: $upstream"
    }

    if ($status.Count -gt 0) {
        Write-Host "Working tree: dirty"
        $status | ForEach-Object { Write-Host "  $_" }
    } else {
        Write-Host "Working tree: clean"
    }

    if ($PullClean) {
        if ($status.Count -gt 0) {
            Write-Warning "Skipping pull for $($project.Name) because the working tree is dirty."
        } else {
            git -C $path pull --ff-only
        }
    }
}
