@echo off
echo Starting VPN container...
docker compose -f docker-compose.vpn.yml up -d

echo Waiting for VPN to be healthy...
:wait_loop
for /f %%s in ('docker inspect --format={{.State.Health.Status}} polymarket-vpn 2^>nul') do set STATUS=%%s
if not "%STATUS%"=="healthy" (
    timeout /t 3 /nobreak >nul
    goto wait_loop
)

echo VPN is healthy. Starting polymarket-copier...
venv\Scripts\python.exe run.py %*
