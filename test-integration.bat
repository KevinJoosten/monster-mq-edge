@echo off
REM Builds the integration tests once to bin\integration.test.exe and runs them from there.
REM The Windows firewall remembers "Allow access" per program path. Plain `go test`
REM builds into a new temp folder on every run, so it asks every time.
REM
REM Usage: test-integration.bat [test binary flags]
REM   test-integration.bat -test.run TestPubSub -test.v

setlocal
cd /d "%~dp0"
if not exist bin mkdir bin
go test -c -o bin\integration.test.exe .\test\integration
if errorlevel 1 exit /b 1
pushd test\integration
..\..\bin\integration.test.exe -test.count=1 -test.timeout=300s %*
set "RC=%ERRORLEVEL%"
popd
exit /b %RC%
