$ErrorActionPreference = 'Stop'

$TaskPrefix = 'HeadroomProxy'
$StartupDir = [Environment]::GetFolderPath('Startup')
$StartupCmd = Join-Path $StartupDir 'Start-Headroom-Proxy.cmd'

Get-ScheduledTask -TaskName "$TaskPrefix-*" -ErrorAction SilentlyContinue | ForEach-Object {
    Unregister-ScheduledTask -TaskName $_.TaskName -Confirm:$false
    Write-Host "Removed scheduled task: $($_.TaskName)"
}

if (Test-Path $StartupCmd) {
    Remove-Item $StartupCmd -Force
    Write-Host "Removed Startup-folder autostart: $StartupCmd"
}

Write-Host 'Headroom autostart entries removed.'
Write-Host 'If the proxy is currently running, stop it manually from its terminal or kill the python process using port 8787.'
