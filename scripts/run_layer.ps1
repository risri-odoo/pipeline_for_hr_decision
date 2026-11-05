# Run one pipeline layer (or all) in Docker. Windows PowerShell 5.1 and PowerShell 7.
#
#   .\scripts\run_layer.ps1 -Layer bronze     # extract (Postgres -> landing) + bronze
#   .\scripts\run_layer.ps1 -Layer silver     # needs bronze\_SUCCESS in the lake
#   .\scripts\run_layer.ps1 -Layer gold       # needs silver\_SUCCESS; then builds site\
#   .\scripts\run_layer.ps1 -Layer all
#
# Switches: -SkipBuild, -SkipSite, -SkipNotebook,
#           -ImportState / -ExportState (lake <-> .lake-state\, as the CI jobs do)
param(
    [Parameter(Mandatory = $true)][ValidateSet("bronze", "silver", "gold", "all")][string]$Layer,
    [switch]$SkipBuild,
    [switch]$SkipSite,
    [switch]$SkipNotebook,
    [switch]$ImportState,
    [switch]$ExportState,
    [string]$LakeStateDir = ".\.lake-state"
)
# "Continue": docker writes progress to stderr, which PowerShell 5.1 would turn into terminating errors.
# Failures are detected through $LASTEXITCODE instead.
$ErrorActionPreference = "Continue"
Set-Location (Join-Path $PSScriptRoot "..")

function Log($msg) { Write-Host "[run_layer $Layer] $msg" }
function Invoke-Docker {
    & docker @args
    if ($LASTEXITCODE -ne 0) { Log "FAILED: docker $($args -join ' ')"; exit 1 }
}
function New-Secret { -join ((1..32) | ForEach-Object { '{0:x}' -f (Get-Random -Maximum 16) }) }

if (-not (Test-Path ".env")) {
    Log "creating .env with random local secrets (git-ignored)"
    $lines = Get-Content ".env.example" | ForEach-Object {
        if ($_ -match '^POSTGRES_PASS=') { "POSTGRES_PASS=$(New-Secret)" }
        elseif ($_ -match '^MINIO_ROOT_PASSWORD=') { "MINIO_ROOT_PASSWORD=$(New-Secret)" }
        else { $_ }
    }
    [IO.File]::WriteAllText((Join-Path (Get-Location) ".env"), (($lines -join "`n") + "`n"), (New-Object Text.UTF8Encoding $false))
}

$env:LAKE_STATE_DIR = $LakeStateDir
New-Item -ItemType Directory -Force $LakeStateDir, "site", "data\generated\learning_activity" | Out-Null

if (-not $SkipBuild) { Log "building images"; Invoke-Docker compose --profile etl build etl reports }

$needsPostgres = $Layer -in @("bronze", "all")
if ($needsPostgres) {
    if (-not (Test-Path "data\generated\manifest.json")) { Log "generating synthetic data"; Invoke-Docker compose run --rm datagen }
    Log "starting postgres + minio"
    Invoke-Docker compose up -d --wait postgres minio
} else {
    Log "starting minio"
    Invoke-Docker compose up -d --wait minio
}

if ($ImportState) {
    Log "importing lake state from $LakeStateDir"
    Invoke-Docker compose run --rm --no-deps etl --import-lake /lake-state
}

Log "running layer"
if ($needsPostgres) { Invoke-Docker compose run --rm -e "LAYER=$Layer" etl }
else { Invoke-Docker compose run --rm --no-deps -e "LAYER=$Layer" etl }

if ($ExportState) {
    Log "exporting lake state to $LakeStateDir"
    Get-ChildItem $LakeStateDir -Force | Remove-Item -Recurse -Force -Confirm:$false
    Invoke-Docker compose run --rm --no-deps etl --export-lake /lake-state
}

if ($Layer -in @("gold", "all") -and -not $SkipSite) {
    Log "building site\"
    if ($SkipNotebook) { $env:SKIP_NOTEBOOK = "1" }
    & docker compose run --rm reports
    $code = $LASTEXITCODE
    Remove-Item Env:SKIP_NOTEBOOK -ErrorAction SilentlyContinue
    if ($code -ne 0) { Log "site build FAILED"; exit 1 }
}

Log "done"
exit 0
