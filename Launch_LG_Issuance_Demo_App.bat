@echo off
title Grow BD — LG Issuance Demo
echo =========================================================
echo   GROW BD - LG Issuance Product Demo (Standalone App)
echo =========================================================
cd /d "%~dp0interactive-experience"

:: Check if port 8080 is already in use
netstat -ano | findstr :8080 >nul
if %ERRORLEVEL% NEQ 0 (
    echo Starting local background server on port 8080...
    start /B python -m http.server 8080 >nul 2>&1
    timeout /t 2 /nobreak >nul
)

echo Launching LG Issuance Demo in native standalone app mode...

:: Attempt to open in Microsoft Edge native App Mode (Borderless, standalone window)
where msedge >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    start msedge --app=http://localhost:8080/#/branch/issuance --window-size=1280,760
    exit /b 0
)

:: Attempt to open in Google Chrome native App Mode
where chrome >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    start chrome --app=http://localhost:8080/#/branch/issuance --window-size=1280,760
    exit /b 0
)

:: Fallback to default browser
start "" "http://localhost:8080/#/branch/issuance"
exit /b 0
