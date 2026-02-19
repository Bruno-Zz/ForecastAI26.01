Set WshShell = WScript.CreateObject("WScript.Shell")
WshShell.Run "cmd /c cd /d C:\allDev\ForecastAI2026.01\files\api && python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload > C:\allDev\ForecastAI2026.01\api_run.log 2>&1", 0, False
