@echo off
title AnimeBot Dashboard
color 0B

echo.
echo  ============================================================
echo   AnimeBot Dashboard  ^|  github.com/xorqie
echo  ============================================================
echo.

:: Check Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Python not found.
    echo  Download Python 3.11+ from https://python.org/downloads
    echo  Make sure to check "Add Python to PATH" during install.
    echo.
    pause
    exit /b 1
)

for /f "tokens=2" %%v in ('python --version 2^>^&1') do set PYVER=%%v
echo  [OK] Python %PYVER% found

:: Install / upgrade dependencies silently
echo  [..] Checking dependencies...
python -m pip install -q --upgrade pip
python -m pip install -q discord.py motor fastapi uvicorn[standard] pymongo
if errorlevel 1 (
    echo  [ERROR] Failed to install dependencies.
    echo  Try running: pip install discord.py motor fastapi uvicorn pymongo
    pause
    exit /b 1
)
echo  [OK] Dependencies ready

echo.

:: First run — no config.json yet
if not exist "config.json" (
    echo  [INFO] No config.json found — starting setup wizard...
    echo.
    python setup.py
    goto :end
)

:: Config exists — start directly
echo  [OK] config.json found
echo  [..] Starting AnimeBot...
echo.
echo  Dashboard: http://127.0.0.1:5050
echo  Press Ctrl+C to stop.
echo  ============================================================
echo.
python discordbot.py

:end
echo.
echo  AnimeBot stopped.
pause
