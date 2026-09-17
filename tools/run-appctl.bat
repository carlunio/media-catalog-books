@echo off
chcp 65001 >nul
setlocal EnableExtensions

set "SCRIPT_DIR=%~dp0"
set "REPO_DIR=%SCRIPT_DIR%.."
set "COMMAND=%~1"
set "ACTION_LABEL=%~2"
set "EXIT_CODE=0"

if "%COMMAND%"=="" (
    echo Uso: %~nx0 ^<comando-appctl^> [etiqueta]
    set "EXIT_CODE=1"
    goto :finish
)

if "%ACTION_LABEL%"=="" set "ACTION_LABEL=%COMMAND%"

call :resolve_python
if errorlevel 1 goto :finish

pushd "%REPO_DIR%" >nul 2>&1
if errorlevel 1 (
    echo No se ha podido abrir la raíz del repositorio: "%REPO_DIR%"
    set "EXIT_CODE=1"
    goto :finish
)

echo %ACTION_LABEL%...
%PYTHON_CMD% "%REPO_DIR%\scripts\appctl.py" "%COMMAND%"
set "EXIT_CODE=%ERRORLEVEL%"

popd >nul 2>&1
goto :finish

:resolve_python
py -3.12 -c "import sys; raise SystemExit(0 if sys.version_info[:2] == (3, 12) else 1)" >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_CMD=py -3.12"
    exit /b 0
)

py -3 -c "import sys; raise SystemExit(0 if sys.version_info.major == 3 and sys.version_info.minor in range(12, 100) else 1)" >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_CMD=py -3"
    exit /b 0
)

python -c "import sys; raise SystemExit(0 if sys.version_info.major == 3 and sys.version_info.minor in range(12, 100) else 1)" >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_CMD=python"
    exit /b 0
)

echo Se requiere Python 3.12 o posterior en el PATH.
set "EXIT_CODE=127"
exit /b 1

:finish
echo.
if "%EXIT_CODE%"=="0" (
    echo Proceso terminado correctamente.
) else (
    echo El comando ha fallado con el código %EXIT_CODE%.
)
echo.
pause
exit /b %EXIT_CODE%
