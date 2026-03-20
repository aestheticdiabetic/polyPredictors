@echo off
echo Starting Polymarket stack...
docker compose -f docker-compose.vpn.yml up -d --build
echo.
echo Done. You can close this window.
echo App: http://localhost:8000
echo.
echo Useful commands:
echo   View logs:  docker logs polymarket-app -f
echo   Stop all:   docker compose -f docker-compose.vpn.yml down
