@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "SCRIPT_DIR=%~dp0"
pushd "%SCRIPT_DIR%" >nul

set "ACTION="
set "PAUSE_ON_ERROR=1"

:parse_args
if "%~1"=="" goto :args_done
if /I "%~1"=="--no-pause" (
    set "PAUSE_ON_ERROR=0"
    shift
    goto :parse_args
)
if not defined ACTION (
    set "ACTION=%~1"
    shift
    goto :parse_args
)
echo Unexpected argument: %~1
goto :help_fail

:args_done
if not defined ACTION set "ACTION=deploy"

if /I "%ACTION%"=="help" goto :help

call :check_prereqs || goto :fail

if /I "%ACTION%"=="deploy" (
    call :deploy_only || goto :fail
    goto :success
)
if /I "%ACTION%"=="vald" (
    call :run_vald || goto :fail
    goto :success
)
if /I "%ACTION%"=="catapult" (
    call :run_catapult || goto :fail
    goto :success
)
if /I "%ACTION%"=="all" (
    call :run_all || goto :fail
    goto :success
)
if /I "%ACTION%"=="status" (
    call :status || goto :fail
    goto :success
)
if /I "%ACTION%"=="logs" (
    call :logs || goto :fail
    goto :success
)
if /I "%ACTION%"=="stop" (
    call :stop || goto :fail
    goto :success
)

echo Unknown action: %ACTION%
goto :help_fail

:check_prereqs
where docker >nul 2>&1 || (
    echo Docker is not installed or not available in PATH.
    exit /b 1
)
docker compose version >nul 2>&1 || (
    echo Docker Compose is not available. Install Docker Desktop with Compose support.
    exit /b 1
)
docker info >nul 2>&1 || (
    echo Docker Desktop is not running or the Docker daemon is unavailable.
    echo Start Docker Desktop, wait for it to finish starting, and run the script again.
    exit /b 1
)
if not exist ".env" (
    echo Missing .env in %SCRIPT_DIR%
    echo Copy the project and configure .env before running this script.
    exit /b 1
)
call :require_env POSTGRES_HOST || exit /b 1
call :require_env POSTGRES_PORT || exit /b 1
call :require_env POSTGRES_DB || exit /b 1
call :require_env POSTGRES_USER || exit /b 1
call :require_env POSTGRES_PASSWORD || exit /b 1
call :require_env VALD_CLIENT_ID || exit /b 1
call :require_env VALD_CLIENT_SECRET || exit /b 1
call :require_env VALD_TOKEN_URL || exit /b 1
call :require_env VALD_REGION || exit /b 1
call :require_env AIRFLOW_ADMIN_USERNAME || exit /b 1
call :require_env AIRFLOW_ADMIN_PASSWORD || exit /b 1
exit /b 0

:require_env
findstr /R /C:"^%~1=." ".env" >nul 2>&1 || (
    echo Missing or empty %~1 in .env
    exit /b 1
)
exit /b 0

:require_catapult_env
call :require_env CATAPULT_BASE_URL || exit /b 1
findstr /R /C:"^CATAPULT_.*_API_KEY=" ".env" >nul 2>&1 || (
    echo Missing at least one CATAPULT_*_API_KEY entry in .env
    exit /b 1
)
exit /b 0

:build_image
echo Building shared Airflow Docker image...
docker compose build airflow-webserver || exit /b 1
exit /b 0

:start_metadata
echo Starting Airflow metadata database...
docker compose up -d airflow-metadata-db || exit /b 1
exit /b 0

:wait_metadata_healthy
set "WAIT_ATTEMPTS=0"
set "MAX_WAIT_ATTEMPTS=36"
echo Waiting for Airflow metadata database to become healthy...
:wait_metadata_loop
set /a WAIT_ATTEMPTS+=1
set "METADATA_CONTAINER_ID="
for /f "usebackq delims=" %%C in (`docker compose ps -q airflow-metadata-db 2^>nul`) do (
    if not defined METADATA_CONTAINER_ID set "METADATA_CONTAINER_ID=%%C"
)
if not defined METADATA_CONTAINER_ID (
    if !WAIT_ATTEMPTS! GEQ !MAX_WAIT_ATTEMPTS! (
        echo Timed out waiting for airflow-metadata-db container to appear.
        exit /b 1
    )
    timeout /t 5 /nobreak >nul
    goto :wait_metadata_loop
)
set "METADATA_STATUS="
for /f "usebackq delims=" %%S in (`docker inspect -f "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" !METADATA_CONTAINER_ID! 2^>nul`) do (
    if not defined METADATA_STATUS set "METADATA_STATUS=%%S"
)
if /I "!METADATA_STATUS!"=="healthy" (
    echo Airflow metadata database is healthy.
    exit /b 0
)
if /I "!METADATA_STATUS!"=="running" (
    echo Airflow metadata database is running.
    exit /b 0
)
if !WAIT_ATTEMPTS! GEQ !MAX_WAIT_ATTEMPTS! (
    echo Timed out waiting for airflow-metadata-db to become healthy.
    exit /b 1
)
timeout /t 5 /nobreak >nul
goto :wait_metadata_loop

:run_airflow_init
echo Initializing Airflow metadata and admin user...
docker compose up --no-deps airflow-init || exit /b 1
exit /b 0

:bootstrap_warehouse
echo Bootstrapping warehouse schemas and tables...
docker compose run --rm airflow-webserver bash -lc "bootstrap_database" || exit /b 1
exit /b 0

:start_services
echo Starting Airflow webserver and scheduler...
docker compose up -d --no-deps airflow-webserver airflow-scheduler || exit /b 1
exit /b 0

:ensure_stack
set "HAS_WEBSERVER="
for /f "usebackq delims=" %%S in (`docker compose ps --services --status running 2^>nul`) do (
    if /I "%%S"=="airflow-webserver" set "HAS_WEBSERVER=1"
)
if not defined HAS_WEBSERVER (
    echo Airflow stack is not running. Deploying it first...
    call :deploy_only || exit /b 1
)
exit /b 0

:deploy_only
call :build_image || exit /b 1
call :start_metadata || exit /b 1
call :wait_metadata_healthy || exit /b 1
call :run_airflow_init || exit /b 1
call :bootstrap_warehouse || exit /b 1
call :start_services || exit /b 1
echo.
echo Docker stack is ready.
echo Airflow UI: http://localhost:8080
echo VALD DAGs will run in Airflow scheduler.
echo Catapult remains available as manual raw/bronze commands in the same container.
exit /b 0

:run_vald
call :ensure_stack || exit /b 1
echo Running VALD ingestion inside Docker...
docker compose exec -T airflow-webserver bash -lc "run_vald_ingestion --runtime-validate" || exit /b 1
exit /b 0

:run_catapult
call :require_catapult_env || exit /b 1
call :ensure_stack || exit /b 1
echo Running Catapult ingestion inside Docker...
docker compose exec -T airflow-webserver bash -lc "run_catapult_ingestion" || exit /b 1
exit /b 0

:run_all
call :require_catapult_env || exit /b 1
call :deploy_only || exit /b 1
call :run_vald || exit /b 1
call :run_catapult || exit /b 1
exit /b 0

:status
docker compose ps
exit /b 0

:logs
docker compose logs -f airflow-webserver airflow-scheduler
exit /b 0

:stop
docker compose down
exit /b 0

:help
echo Usage:
echo   deploy_performance_stack.bat [deploy^|vald^|catapult^|all^|status^|logs^|stop^|help] [--no-pause]
echo.
echo Actions:
echo   deploy    Build the image, initialize Airflow, bootstrap the warehouse, and start the stack.
echo   vald      Run the VALD ingestion once inside the running Docker stack.
echo   catapult  Run the Catapult ingestion once inside the running Docker stack.
echo   all       Deploy the stack, then run VALD and Catapult once.
echo   status    Show Docker Compose service status.
echo   logs      Tail Airflow webserver and scheduler logs.
echo   stop      Stop the Docker Compose stack.
echo   help      Show this help message.
echo.
echo Requirements:
echo   1. Docker Desktop with Compose.
echo   2. A configured .env file in this project folder.
echo   3. External warehouse PostgreSQL reachable from Docker.
echo.
echo Notes:
echo   - VALD is scheduled in Airflow once the stack is up.
echo   - Catapult in this repo is raw/bronze only and runs on demand.
goto :success

:help_fail
call :show_pause_hint
goto :fail

:show_pause_hint
if "%PAUSE_ON_ERROR%"=="1" (
    echo.
    echo This window will stay open so you can read the error.
)
exit /b 0

:pause_if_error
if "%PAUSE_ON_ERROR%"=="1" pause
exit /b 0

:success
popd >nul
endlocal
exit /b 0

:fail
echo.
echo Script failed. Read the message above.
call :pause_if_error
popd >nul
endlocal
exit /b 1
