@echo off
title PROJECT HANNIBAL — OSINT COMMAND MATRIX
chcp 65001 > nul
cd /d "%~dp0"
cls
echo [*] Initializing PROJECT HANNIBAL Runtime Environment...
python HBL5.py
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [!] An unexpected execution halt occurred.
    echo [!] Diagnostic code: %ERRORLEVEL%
    pause
)
