@echo off
cd /d D:\sourcecode\quant
echo ---------------------------------------- >> data\collect.log
echo %date% %time% >> data\collect.log
echo ---------------------------------------- >> data\collect.log
C:\Python314\Scripts\uv.exe run python download_etf_minute_data.py >> data\collect.log 2>&1
echo Exit code: %errorlevel% >> data\collect.log
