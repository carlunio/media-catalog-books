@echo off
chcp 65001 >nul
call "%~dp0run-appctl.bat" update-and-launch "Comprobando actualizaciones y arrancando la aplicación"
