@echo off
REM -----------------------------
REM YOLO Multi-Camera Demo Launcher
REM -----------------------------

REM Portable Python path
set PYTHON_BIN=python\python.exe

REM Path to the unified demo script
set SCRIPT=scripts\multi_camera_demo.py

echo --------------------------------------------------
echo YOLO Multi-Camera Demo
echo --------------------------------------------------
echo.

REM Run the Python script
%PYTHON_BIN% %SCRIPT%

echo.
echo Demo finished. Press any key to close this window.
pause
