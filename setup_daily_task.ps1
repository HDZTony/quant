#Requires -RunAsAdministrator

$TaskName    = "ETF159506_1min_Collector"
$ProjectDir  = "D:\sourcecode\quant"
$BatFile     = "$ProjectDir\collect_1min.bat"
$TriggerTime = "15:05"

$Action = New-ScheduledTaskAction `
    -Execute $BatFile `
    -WorkingDirectory $ProjectDir

$Trigger = New-ScheduledTaskTrigger `
    -Weekly -DaysOfWeek Monday, Tuesday, Wednesday, Thursday, Friday `
    -At $TriggerTime

$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 10)

$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "[OK] Removed old task"
}

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Description "Daily ETF 159506 1min kline incremental collector" `
    -RunLevel Highest

Write-Host ""
Write-Host "===== Scheduled Task Registered ====="
Write-Host ("  Task:      " + $TaskName)
Write-Host ("  Schedule:  Mon-Fri " + $TriggerTime)
Write-Host ("  Executes:  " + $BatFile)
Write-Host ""
Write-Host ("  View:   schtasks /query /tn " + $TaskName)
Write-Host ("  Run:    schtasks /run /tn " + $TaskName)
Write-Host ("  Delete: schtasks /delete /tn " + $TaskName + " /f")
