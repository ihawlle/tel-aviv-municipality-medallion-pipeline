# Deploy the medallion pipeline bundle to Databricks (dev target).
# Run from the repository root (parent of this scripts folder).
# Prerequisites: Databricks CLI installed and authenticated.
#   Install: https://docs.databricks.com/dev-tools/cli/index.html

$ErrorActionPreference = "Stop"
# Ensure PATH includes WinGet/User paths so databricks is found
$env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path", "User")
$Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
Set-Location $Root

if (-not (Get-Command databricks -ErrorAction SilentlyContinue)) {
    Write-Error "Databricks CLI not found. Install it first: https://docs.databricks.com/dev-tools/cli/index.html"
    exit 1
}

Write-Host "Validating bundle..."
databricks bundle validate
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Deploying to target: dev"
databricks bundle deploy -t dev
exit $LASTEXITCODE
