$ErrorActionPreference = 'Stop'

$TaskPrefix = 'HeadroomProxy'
$EnsureScript = Join-Path $PSScriptRoot 'ensure-headroom.ps1'
$StartupDir = [Environment]::GetFolderPath('Startup')
$StartupCmd = Join-Path $StartupDir 'Start-Headroom-Proxy.cmd'

if (-not (Test-Path $EnsureScript)) {
    throw "Missing ensure script: $EnsureScript"
}

function Install-StartupFolderFallback {
    New-Item -ItemType Directory -Force -Path $StartupDir | Out-Null
    $content = @"
@echo off
powershell.exe -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File "$EnsureScript"
"@
    Set-Content -Path $StartupCmd -Value $content -Encoding ASCII
    Write-Host "Installed Startup-folder autostart: $StartupCmd"
}

try {
    $Action = New-ScheduledTaskAction `
        -Execute 'powershell.exe' `
        -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$EnsureScript`""

    $LogonTrigger = New-ScheduledTaskTrigger -AtLogOn
    $HealthTrigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) `
        -RepetitionInterval (New-TimeSpan -Minutes 5) `
        -RepetitionDuration (New-TimeSpan -Days 3650)

    $Settings = New-ScheduledTaskSettingsSet `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -StartWhenAvailable `
        -MultipleInstances IgnoreNew

    $Principal = New-ScheduledTaskPrincipal `
        -UserId $env:USERNAME `
        -LogonType Interactive `
        -RunLevel Limited

    Register-ScheduledTask `
        -TaskName "$TaskPrefix-AtLogon" `
        -Action $Action `
        -Trigger $LogonTrigger `
        -Settings $Settings `
        -Principal $Principal `
        -Description 'Start Headroom proxy when the user logs on.' `
        -Force | Out-Null

    Register-ScheduledTask `
        -TaskName "$TaskPrefix-HealthCheck" `
        -Action $Action `
        -Trigger $HealthTrigger `
        -Settings $Settings `
        -Principal $Principal `
        -Description 'Ensure Headroom proxy is running every 5 minutes.' `
        -Force | Out-Null

    Write-Host 'Registered scheduled tasks:'
    Get-ScheduledTask -TaskName "$TaskPrefix-*" | Select-Object TaskName,State | Format-Table -AutoSize
} catch {
    Write-Warning "Could not register Windows scheduled tasks: $($_.Exception.Message)"
    Write-Warning 'Falling back to the current user Startup folder.'
    Install-StartupFolderFallback
}

Write-Host 'Starting Headroom now...'
& $EnsureScript
