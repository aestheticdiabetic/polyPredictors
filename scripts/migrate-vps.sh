#!/usr/bin/env bash
# =============================================================================
# migrate-vps.sh — Migrate polyPredictors to a new Hetzner VPS
#
# Run from your HOME MACHINE (Git Bash on Windows):
#   bash scripts/migrate-vps.sh <NEW_IP> [OLD_IP]
#
# Arguments:
#   NEW_IP  — IP of the new server (e.g. Helsinki)
#   OLD_IP  — IP of the old server (e.g. Frankfurt) — optional
#             If provided, the database is copied server-to-server.
#             If omitted, the local ./data/polymarket_copier.db is used.
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Config — edit these if your setup differs
# ---------------------------------------------------------------------------
NEW_IP="${1:?ERROR: Provide new server IP as first argument: bash migrate-vps.sh <NEW_IP> [OLD_IP]}"
OLD_IP="${2:-}"
SSH_USER="angus"
APP_DIR="/home/$SSH_USER/polyPredictors"
REPO_URL="https://github.com/aestheticdiabetic/polyPredictors.git"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_ENV="$SCRIPT_DIR/../.env"
LOCAL_DB="$SCRIPT_DIR/../data/polymarket_copier.db"

SSH_OPTS="-o StrictHostKeyChecking=accept-new -o ConnectTimeout=10"

# ---------------------------------------------------------------------------
print_step() { echo; echo "===> $1"; echo; }
# ---------------------------------------------------------------------------

print_step "Phase 1/5 — Initial server setup (as root)"

ssh $SSH_OPTS root@"$NEW_IP" bash <<'REMOTE_ROOT'
set -euo pipefail

echo "--- Updating packages ---"
apt-get update -qq && apt-get upgrade -y -qq

echo "--- Configuring firewall ---"
apt-get install -y -qq ufw
ufw --force reset
ufw default deny incoming
ufw default allow outgoing
ufw allow ssh
ufw --force enable

echo "--- Hardening SSH ---"
sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config
sed -i 's/^#\?PubkeyAuthentication.*/PubkeyAuthentication yes/' /etc/ssh/sshd_config
sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin prohibit-password/' /etc/ssh/sshd_config
systemctl restart ssh

echo "--- Creating user angus ---"
if ! id angus &>/dev/null; then
    adduser --disabled-password --gecos "" angus
fi
usermod -aG sudo angus

echo "--- Copying SSH keys to angus ---"
mkdir -p /home/angus/.ssh
cp -f /root/.ssh/authorized_keys /home/angus/.ssh/authorized_keys
chown -R angus:angus /home/angus/.ssh
chmod 700 /home/angus/.ssh
chmod 600 /home/angus/.ssh/authorized_keys

echo "--- Installing Docker ---"
curl -fsSL https://get.docker.com -o /tmp/get-docker.sh
sh /tmp/get-docker.sh
usermod -aG docker angus

echo "--- Adding swap (2GB) ---"
if [ ! -f /swapfile ]; then
    fallocate -l 2G /swapfile
    chmod 600 /swapfile
    mkswap /swapfile
    swapon /swapfile
    echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

echo "--- Phase 1 complete ---"
REMOTE_ROOT

print_step "Phase 2/5 — Clone repo and create directories (as angus)"

ssh $SSH_OPTS "$SSH_USER@$NEW_IP" bash <<REMOTE_ANGUS
set -euo pipefail

if [ -d "$APP_DIR/.git" ]; then
    echo "Repo already exists — pulling latest"
    cd "$APP_DIR" && git pull
else
    echo "Cloning repo"
    git clone "$REPO_URL" "$APP_DIR"
fi

mkdir -p "$APP_DIR/data" "$APP_DIR/logs"
echo "--- Phase 2 complete ---"
REMOTE_ANGUS

print_step "Phase 3/5 — Copying .env file"

if [ ! -f "$LOCAL_ENV" ]; then
    echo "ERROR: .env not found at $LOCAL_ENV"
    exit 1
fi

scp $SSH_OPTS "$LOCAL_ENV" "$SSH_USER@$NEW_IP:$APP_DIR/.env"
ssh $SSH_OPTS "$SSH_USER@$NEW_IP" "chmod 600 $APP_DIR/.env"
echo ".env copied and permissions locked (600)"

print_step "Phase 4/5 — Copying database"

if [ -n "$OLD_IP" ]; then
    echo "Downloading database from old server ($OLD_IP) via home machine"
    echo "(stopping bot on old server first)"
    ssh $SSH_OPTS "$SSH_USER@$OLD_IP" "cd $APP_DIR && docker compose stop 2>/dev/null || true"
    mkdir -p "$(dirname "$LOCAL_DB")"
    scp $SSH_OPTS "$SSH_USER@$OLD_IP:$APP_DIR/data/polymarket_copier.db" "$LOCAL_DB"
    echo "Uploading database to new server ($NEW_IP)"
    scp $SSH_OPTS "$LOCAL_DB" "$SSH_USER@$NEW_IP:$APP_DIR/data/polymarket_copier.db"
    echo "Database copied via home machine relay"
elif [ -f "$LOCAL_DB" ]; then
    echo "Copying local database"
    scp $SSH_OPTS "$LOCAL_DB" "$SSH_USER@$NEW_IP:$APP_DIR/data/polymarket_copier.db"
    echo "Local database copied"
else
    echo "WARNING: No database found locally or old server IP provided — starting fresh"
fi

print_step "Phase 5/5 — Building and starting the bot"

ssh $SSH_OPTS "$SSH_USER@$NEW_IP" bash <<REMOTE_START
set -euo pipefail
cd "$APP_DIR"
docker compose up -d --build
sleep 3
echo ""
echo "--- Container status ---"
docker compose ps
echo ""
echo "--- Last 20 log lines ---"
docker compose logs --tail=20
REMOTE_START

echo ""
echo "=============================================="
echo " Migration complete!"
echo " New server: $NEW_IP"
echo " Dashboard:  ssh -L 8081:localhost:8081 $SSH_USER@$NEW_IP"
echo "=============================================="
echo ""
if [ -n "$OLD_IP" ]; then
    echo "Old server ($OLD_IP) is stopped but NOT deleted."
    echo "Verify everything works, then delete it in the Hetzner console."
fi
