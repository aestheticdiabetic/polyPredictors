# VPS Commands — Hetzner Helsinki

**Server IP:** 95.216.211.236
**User:** angus
**App directory:** /home/angus/polyPredictors
**Dashboard:** http://localhost:8081 (via SSH tunnel only)

> Old Frankfurt server (95.216.211.236) — delete from Hetzner console once Helsinki is verified

---

## Connect & Dashboard

```powershell
# SSH into VPS
ssh angus@95.216.211.236

# Open dashboard tunnel (keep terminal open, then browse to http://localhost:8081)
ssh -L 8081:localhost:8081 angus@95.216.211.236
```

---

## Docker — Daily Usage

```bash
# Start bot (build image + launch in background)
docker compose up -d --build

# Start without rebuilding
docker compose up -d

# Stop bot
docker compose down

# Restart bot
docker compose restart

# Check container status
docker compose ps
```

---

## Logs

```bash
# Stream live logs
docker compose logs -f

# Last 100 lines
docker compose logs --tail=100

# Last 100 lines and stream
docker compose logs --tail=100 -f

# Trade activity only (entries, exits, resolutions, errors)
docker compose logs -f | grep -E "(Placed (SIM|REAL|HEDGE)|REAL BUY:|REAL SELL|SIM SELL:|Resolved bet|Force-close:|Near-expiry close:|Oracle timeout:|follow_add_signals: placed|Drift watchlist: RETRY SUCCESS|_close_all_tranches: bet|Add-to-position:|New price tranche:|Retry BUY succeeded|Failed to place bet)"

# Last 500 lines — save to file for analysis
ssh angus@95.216.211.236 "docker compose -f /home/angus/polyPredictors/docker-compose.yml logs --tail=500" > "x:\CODING\polymarket-copier\logs\app_$(Get-Date -Format 'yyyyMMdd_HHmm').txt"
```

---

## Deploy Code Changes

```powershell
# On your HOME MACHINE — push changes
git push
```

```bash
# On VPS — pull and restart
cd /home/angus/polyPredictors
git pull && docker compose up -d --build
```

---

## Database

```bash
# Backup database on VPS
cp /home/angus/polyPredictors/data/polymarket_copier.db \
   /home/angus/polyPredictors/data/polymarket_copier.db.bak

# Open database directly
sqlite3 /home/angus/polyPredictors/data/polymarket_copier.db

# Useful SQL queries
.tables
SELECT * FROM copied_bets ORDER BY id DESC LIMIT 10;
SELECT status, COUNT(*) FROM copied_bets GROUP BY status;
```

```powershell
# Download database to home machine (run on HOME MACHINE)
scp angus@95.216.211.236:/home/angus/polyPredictors/data/polymarket_copier.db "x:\CODING\polymarket-copier\backups\polymarket_copier_$(Get-Date -Format 'yyyyMMdd').db"

# Upload database to VPS (stop bot first)
scp "x:\CODING\polymarket-copier\data\polymarket_copier.db" angus@95.216.211.236:/home/angus/polyPredictors/data/polymarket_copier.db
```

---

## .env Management

```bash
# Edit .env on VPS
nano /home/angus/polyPredictors/.env

# Restart bot after .env changes
cd /home/angus/polyPredictors && docker compose up -d
```

```powershell
# Push updated .env from home machine to VPS
scp "x:\CODING\polymarket-copier\.env" angus@95.216.211.236:/home/angus/polyPredictors/.env
```

---

## Debugging

```bash
# Open shell inside running container
docker exec -it polymarket-app bash

# Check disk usage
df -h

# Check memory usage
free -h

# Check CPU / process usage
htop
```

---

## Firewall

```bash
# View firewall rules
sudo ufw status

# Add a rule (e.g. if you ever need another port)
sudo ufw allow PORT

# Remove a rule
sudo ufw delete allow PORT
```

---

## Server Maintenance

```bash
# Update system packages
sudo apt update && sudo apt upgrade -y

# Reboot server (bot will auto-restart via restart: unless-stopped)
sudo reboot
```

---

## Fix Permissions (if scp fails with Permission Denied)

```bash
sudo chown -R angus:angus /home/angus/polyPredictors
```

---

## Recovery (if SSH breaks)

Use Hetzner's browser console:
Hetzner Cloud Console → Your Server → **Console** tab (top right)
Login as `angus` with your password, or `root` if needed.

---

## Migrate to a New VPS

```bash
# Run from Git Bash on home machine
# Usage: bash scripts/migrate-vps.sh <NEW_IP> [OLD_IP]
bash scripts/migrate-vps.sh <NEW_IP> 95.216.211.236
```
