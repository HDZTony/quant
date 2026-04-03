@echo off
cd /d D:\sourcecode\quant
echo ---------------------------------------- >> data\live_trading.log
echo %date% %time% >> data\live_trading.log
echo ---------------------------------------- >> data\live_trading.log
C:\Python314\Scripts\uv.exe run python etf_159506_live_trading.py --mode production >> data\live_trading.log 2>&1
echo Exit code: %errorlevel% >> data\live_trading.log
