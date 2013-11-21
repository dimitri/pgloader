@echo off
echo Chinook Database Version 1.4
echo.

if "%1"=="" goto MENU
if not exist %1 goto ERROR

set SQLFILE=%1
goto RUNSQL

:ERROR
echo The file %1 does not exist.
echo.
goto END

:MENU
echo Options:
echo.
echo 1. Run Chinook_Sqlite.sql
echo 2. Run Chinook_Sqlite_AutoIncrementPKs.sql
echo 3. Exit
echo.
choice /c 123
if (%ERRORLEVEL%)==(1) set SQLFILE=Chinook_Sqlite.sql
if (%ERRORLEVEL%)==(2) set SQLFILE=Chinook_Sqlite_AutoIncrementPKs.sql
if (%ERRORLEVEL%)==(3) goto END

:RUNSQL
echo.
echo Running %SQLFILE%...
if exist %SQLFILE%ite del %SQLFILE%ite
sqlite3 -init %SQLFILE% %SQLFILE%ite

:END
echo.
set SQLFILE=

