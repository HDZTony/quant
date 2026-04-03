@echo off
cd /d D:\sourcecode\quant
echo ---------------------------------------- >> data\collect_tick.log
echo %date% %time% >> data\collect_tick.log
echo ---------------------------------------- >> data\collect_tick.log
C:\Python314\Scripts\uv.exe run python collect_tick_only.py >> data\collect_tick.log 2>&1
echo Exit code: %errorlevel% >> data\collect_tick.log
