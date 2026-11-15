@echo off
chcp 65001 >nul
cd /d "%~dp0"
if not exist .venv\Scripts\python.exe (
  echo 找不到 .venv，請先執行： python -m venv .venv ^&^& .venv\Scripts\pip install -r requirements.txt
  pause
  exit /b 1
)
docker compose up -d
start "dispatcher-1" cmd /k .venv\Scripts\python dispatcher.py --name dispatcher-1
start "dispatcher-2" cmd /k .venv\Scripts\python dispatcher.py --name dispatcher-2
start "simulator" cmd /k .venv\Scripts\python simulator.py --cars 100 --speed 150
start "backend" cmd /k .venv\Scripts\python -m uvicorn app:app --port 8000
timeout /t 4 >nul
start http://localhost:8000/dashboard
start http://localhost:8000/
