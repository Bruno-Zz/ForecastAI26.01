@echo off
cd /d C:\allDev\ForecastAI2026.01\files
C:\allDev\ForecastAI2026.01\files\venv\Scripts\python.exe -m uvicorn api.main:app --host 127.0.0.1 --port 8002 --reload
