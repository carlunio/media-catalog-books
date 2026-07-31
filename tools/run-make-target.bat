@echo off
chcp 65001 >nul
setlocal EnableExtensions

set "SCRIPT_DIR=%~dp0"
set "REPO_DIR=%SCRIPT_DIR%.."
set "TARGET=%~1"
set "ACTION_LABEL=%~2"
set "EXIT_CODE=0"

if "%TARGET%"=="" (
    echo Uso: %~nx0 ^<make-target^> [etiqueta]
    set "EXIT_CODE=1"
    goto :finish
)

if "%ACTION_LABEL%"=="" set "ACTION_LABEL=%TARGET%"

call :resolve_make
if errorlevel 1 goto :finish

pushd "%REPO_DIR%" >nul 2>&1
if errorlevel 1 (
    echo No se ha podido abrir la raíz del repositorio: "%REPO_DIR%"
    set "EXIT_CODE=1"
    goto :finish
)

echo %ACTION_LABEL%...
%MAKE_CMD% %TARGET%
set "EXIT_CODE=%ERRORLEVEL%"

popd >nul 2>&1

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

:resolve_make
where /Q make.exe
if not errorlevel 1 (
    set "MAKE_CMD=make"
    exit /b 0
)

where /Q mingw32-make.exe
if not errorlevel 1 (
    set "MAKE_CMD=mingw32-make"
    exit /b 0
)

where /Q gmake.exe
if not errorlevel 1 (
    set "MAKE_CMD=gmake"
    exit /b 0
)

echo GNU Make no está disponible en el PATH.
echo Instálalo y vuelve a intentarlo. Nombres admitidos: make, mingw32-make o gmake.
set "EXIT_CODE=127"
exit /b 1
