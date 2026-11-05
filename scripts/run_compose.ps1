# Windows PowerShell version of scripts/run_compose.sh (works in Windows PowerShell 5.1 and PowerShell 7).
# Runs the pipeline in Docker (Postgres + MinIO + etl), then builds site/ (dashboard + notebook).
#
#   .\scripts\run_compose.ps1                 # full run + site
#   .\scripts\run_compose.ps1 -Runs 2         # run twice (idempotency check)
#   .\scripts\run_compose.ps1 -Reset          # drop volumes first (fresh Postgres + empty lake); needed after regenerating data
#   .\scripts\run_compose.ps1 -SkipSite       # pipeline only
#   .\scripts\run_compose.ps1 -Findings       # print the headline numbers afterwards
param(
    [int]$Runs = 1,
    [switch]$Reset,
    [switch]$SkipBuild,
    [switch]$SkipSite,
    [switch]$SkipNotebook,
    [switch]$Findings
)
# "Continue": docker writes progress to stderr, which PowerShell 5.1 would turn into terminating errors.
# Failures are detected through $LASTEXITCODE instead.
$ErrorActionPreference = "Continue"
Set-Location (Join-Path $PSScriptRoot "..")

function Log($msg) { Write-Host "[run_compose] $msg" }
function Invoke-Docker {
    & docker @args
    if ($LASTEXITCODE -ne 0) { throw "docker $($args -join ' ') failed (exit $LASTEXITCODE)" }
}
function New-Secret { -join ((1..32) | ForEach-Object { '{0:x}' -f (Get-Random -Maximum 16) }) }

if (-not (Test-Path ".env")) {
    Log "creating .env with random local secrets (git-ignored)"
    $lines = Get-Content ".env.example" | ForEach-Object {
        if ($_ -match '^POSTGRES_PASS=') { "POSTGRES_PASS=$(New-Secret)" }
        elseif ($_ -match '^MINIO_ROOT_PASSWORD=') { "MINIO_ROOT_PASSWORD=$(New-Secret)" }
        else { $_ }
    }
    # UTF-8 without BOM, LF endings: docker compose reads it as-is
    [IO.File]::WriteAllText((Join-Path (Get-Location) ".env"), (($lines -join "`n") + "`n"), (New-Object Text.UTF8Encoding $false))
}

if ($Reset) { Log "removing containers and volumes"; Invoke-Docker compose --profile etl down -v --remove-orphans }
if (-not $SkipBuild) { Log "building etl + reports images"; Invoke-Docker compose --profile etl build etl reports }

New-Item -ItemType Directory -Force "data\generated\learning_activity" | Out-Null
if (-not (Test-Path "data\generated\manifest.json")) { Log "generating synthetic data"; Invoke-Docker compose run --rm datagen }

Log "starting postgres + minio"
Invoke-Docker compose up -d --wait postgres minio

$ok = $true
for ($i = 1; $i -le $Runs; $i++) {
    Log "etl run $i/$Runs"
    & docker compose run --rm etl
    if ($LASTEXITCODE -ne 0) { $ok = $false; break }
}

if ($Findings -or -not $ok) {
    & docker compose run --rm --no-deps --entrypoint python etl scripts/findings.py
}

if ($ok -and -not $SkipSite) {
    Log "building site\ (dashboard + notebook)"
    New-Item -ItemType Directory -Force site | Out-Null
    if ($SkipNotebook) { $env:SKIP_NOTEBOOK = "1" }
    & docker compose run --rm reports
    Remove-Item Env:SKIP_NOTEBOOK -ErrorAction SilentlyContinue
    if ($LASTEXITCODE -ne 0) { Log "site build FAILED"; $ok = $false } else { Log "site ready: site\index.html" }
}

if ($ok) { Log "pipeline SUCCEEDED"; exit 0 } else { Log "pipeline FAILED"; exit 1 }
