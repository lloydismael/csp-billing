$ErrorActionPreference = 'Stop'

$EnsureScript = Join-Path $PSScriptRoot 'ensure-headroom.ps1'

if (-not (Test-Path $EnsureScript)) {
    throw "Missing Headroom ensure script: $EnsureScript"
}

& $EnsureScript
