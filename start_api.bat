@echo off
cd /d C:\allDev\ForecastAI2026.01\files\api
python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload
