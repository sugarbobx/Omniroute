@echo off
cd /d C:\Omniroute
C:\Omniroute\venv\Scripts\python.exe -m uvicorn main:app --host 0.0.0.0 --port 8000 --lifespan off
