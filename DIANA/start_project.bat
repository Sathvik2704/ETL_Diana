@echo off
echo ==============================================
echo Starting DIANA ETL Backend (FastAPI)...
echo ==============================================
start cmd /k "cd /d %~dp0 && .\.venv\Scripts\activate && python -m uvicorn main:app --port 8000"

echo ==============================================
echo Starting DIANA React Frontend (Vite)...
echo ==============================================
start cmd /k "cd /d %~dp0\frontend && npm run dev"

echo All systems initiated! 
echo Frontend: http://localhost:5173
echo Backend:  http://localhost:8000
pause
