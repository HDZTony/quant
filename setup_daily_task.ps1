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
Write-Host "===== 1min Collector Task Registered ====="
Write-Host ("  Task:      " + $TaskName)
Write-Host ("  Schedule:  Mon-Fri " + $TriggerTime)
Write-Host ("  Executes:  " + $BatFile)
Write-Host ""

# -------------------------------------------------------
# Task 2: Live Trading (adapter + strategy + tick saver)
# -------------------------------------------------------
$LiveTaskName    = "ETF159506_Live_Trading"
$LiveBatFile     = "$ProjectDir\start_live_trading.bat"
$LiveTriggerTime = "09:20"

$LiveAction = New-ScheduledTaskAction `
    -Execute $LiveBatFile `
    -WorkingDirectory $ProjectDir

$LiveTrigger = New-ScheduledTaskTrigger `
    -Weekly -DaysOfWeek Monday, Tuesday, Wednesday, Thursday, Friday `
    -At $LiveTriggerTime

$LiveSettings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 6)

$existingLive = Get-ScheduledTask -TaskName $LiveTaskName -ErrorAction SilentlyContinue
if ($existingLive) {
    Unregister-ScheduledTask -TaskName $LiveTaskName -Confirm:$false
    Write-Host "[OK] Removed old live trading task"
}

Register-ScheduledTask `
    -TaskName $LiveTaskName `
    -Action $LiveAction `
    -Trigger $LiveTrigger `
    -Settings $LiveSettings `
    -Description "Daily ETF 159506 live trading (adapter + strategy + tick collection)" `
    -RunLevel Highest

Write-Host ""
Write-Host "===== Live Trading Task Registered ====="
Write-Host ("  Task:      " + $LiveTaskName)
Write-Host ("  Schedule:  Mon-Fri " + $LiveTriggerTime)
Write-Host ("  Executes:  " + $LiveBatFile)
Write-Host ("  Timeout:   6 hours")
Write-Host ""

# -------------------------------------------------------
# Task 3: Tick Data Collector (independent from trading)
# -------------------------------------------------------
$TickTaskName    = "ETF159506_Tick_Collector"
$TickBatFile     = "$ProjectDir\collect_tick.bat"
$TickTriggerTime = "09:10"

$TickAction = New-ScheduledTaskAction `
    -Execute $TickBatFile `
    -WorkingDirectory $ProjectDir

$TickTrigger = New-ScheduledTaskTrigger `
    -Weekly -DaysOfWeek Monday, Tuesday, Wednesday, Thursday, Friday `
    -At $TickTriggerTime

$TickSettings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 7)

$existingTick = Get-ScheduledTask -TaskName $TickTaskName -ErrorAction SilentlyContinue
if ($existingTick) {
    Unregister-ScheduledTask -TaskName $TickTaskName -Confirm:$false
    Write-Host "[OK] Removed old tick collector task"
}

Register-ScheduledTask `
    -TaskName $TickTaskName `
    -Action $TickAction `
    -Trigger $TickTrigger `
    -Settings $TickSettings `
    -Description "Daily ETF 159506 tick data collector (independent, auto-reconnect)" `
    -RunLevel Highest

Write-Host ""
Write-Host "===== Tick Collector Task Registered ====="
Write-Host ("  Task:      " + $TickTaskName)
Write-Host ("  Schedule:  Mon-Fri " + $TickTriggerTime)
Write-Host ("  Executes:  " + $TickBatFile)
Write-Host ("  Timeout:   7 hours")
Write-Host ""

# -------------------------------------------------------
# Summary
# -------------------------------------------------------
Write-Host "===== All Tasks ====="
Write-Host ("  View 1min:  schtasks /query /tn " + $TaskName)
Write-Host ("  View Live:  schtasks /query /tn " + $LiveTaskName)
Write-Host ("  View Tick:  schtasks /query /tn " + $TickTaskName)
Write-Host ("  Run 1min:   schtasks /run /tn " + $TaskName)
Write-Host ("  Run Live:   schtasks /run /tn " + $LiveTaskName)
Write-Host ("  Run Tick:   schtasks /run /tn " + $TickTaskName)
