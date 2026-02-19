@echo off
echo ============================================
echo  ForecastAI2026 - Starting servers
echo ============================================

REM Kill any existing instances
echo Stopping existing servers...
for /f "tokens=5" %%p in ('netstat -ano 2^>nul ^| findstr :8002 ^| findstr LISTENING') do taskkill /F /PID %%p >nul 2>&1
for /f "tokens=5" %%p in ('netstat -ano 2^>nul ^| findstr :5173 ^| findstr LISTENING') do taskkill /F /PID %%p >nul 2>&1

REM Give processes time to release ports
ping -n 2 127.0.0.1 >nul

REM Start API server (port 8002)
echo Starting API on port 8002...
start "ForecastAI - API" /min cmd /c "cd /d %~dp0files && python -m uvicorn api.main:app --host 127.0.0.1 --port 8002"

REM Give API time to start
ping -n 5 127.0.0.1 >nul

REM Start Vite frontend (port 5173 -> proxies to 8002)
echo Starting frontend on port 5173...
start "ForecastAI - Frontend" /min cmd /c "cd /d %~dp0files\frontend && npm run dev"

REM Wait for frontend
ping -n 5 127.0.0.1 >nul

echo.
echo ============================================
echo  App ready at: http://localhost:5173
echo  API ready at: http://localhost:8002
echo ============================================
echo  Close the two minimized terminal windows to stop.
echo ============================================
