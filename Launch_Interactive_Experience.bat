    @echo off
title Grow BD — Interactive Treasury Experience
echo ===================================================
echo   GROW BD - Interactive Treasury Experience
echo   Starting local server & opening browser...
echo ===================================================
cd /d "%~dp0interactive-experience"
start "" "http://localhost:8080"
python -m http.server 8080
pause
