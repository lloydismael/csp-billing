$ErrorActionPreference = 'Stop'

$HeadroomPython = Join-Path $env:APPDATA 'uv\tools\headroom-ai\Scripts\python.exe'
$HostName = '127.0.0.1'
$Port = '8787'
$HealthUrl = "http://${HostName}:${Port}/health"
$LogDir = Join-Path $env:LOCALAPPDATA 'Headroom'
$LogPath = Join-Path $LogDir 'headroom-proxy.log'

if (-not (Test-Path $HeadroomPython)) {
    throw "Headroom Python environment was not found at: $HeadroomPython. Install it with: uv tool install --python 3.13 `"headroom-ai[all]`""
}

try {
    $health = Invoke-RestMethod $HealthUrl -TimeoutSec 3
    if ($health.status -eq 'healthy' -and $health.ready -eq $true) {
        Write-Host "Headroom proxy already healthy at $HealthUrl"
        exit 0
    }
} catch {
    # Not running or not ready; start below.
}

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$env:HEADROOM_OUTPUT_SHAPER = '1'
$env:HEADROOM_TELEMETRY = 'off'
$env:HEADROOM_HOST = $HostName
$env:HEADROOM_PORT = $Port

$arguments = @(
    '-NoProfile',
    '-ExecutionPolicy',
    'Bypass',
    '-Command',
    "`$env:HEADROOM_OUTPUT_SHAPER='1'; `$env:HEADROOM_TELEMETRY='off'; & '$HeadroomPython' -c 'from headroom.cli import main; main()' proxy --host $HostName --port $Port *> '$LogPath'"
)

Start-Process -FilePath 'powershell.exe' -ArgumentList $arguments -WindowStyle Hidden
Start-Sleep -Seconds 5

try {
    $health = Invoke-RestMethod $HealthUrl -TimeoutSec 5
    if ($health.status -eq 'healthy' -and $health.ready -eq $true) {
        Write-Host "Headroom proxy started and healthy at $HealthUrl"
        exit 0
    }
    throw "Headroom proxy started but is not healthy. Status: $($health.status), ready: $($health.ready)"
} catch {
    throw "Headroom proxy failed health check after start. See log: $LogPath. $($_.Exception.Message)"
}
