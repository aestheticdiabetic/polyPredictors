#!/usr/bin/env bash
# =============================================================================
# validate-vps-security.sh — Verify all security settings on the VPS
#
# Run from Git Bash on your home machine:
#   bash scripts/validate-vps-security.sh [SERVER_IP]
#
# Defaults to the Helsinki VPS if no IP is provided.
# =============================================================================

SERVER_IP="${1:-95.216.211.236}"
SSH_USER="angus"
APP_DIR="/home/angus/polyPredictors"
SOCKET="/tmp/vps-validate-$$"
SSH_OPTS="-o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 -o ControlMaster=auto -o ControlPath=$SOCKET -o ControlPersist=60"

# Open a single multiplexed connection (one passphrase prompt)
ssh $SSH_OPTS -fN "$SSH_USER@$SERVER_IP"
trap "ssh -O exit -o ControlPath=$SOCKET $SSH_USER@$SERVER_IP 2>/dev/null; rm -f $SOCKET" EXIT

PASS=0
FAIL=0

pass() { echo "  [PASS] $1"; PASS=$((PASS + 1)); }
fail() { echo "  [FAIL] $1"; FAIL=$((FAIL + 1)); }
section() { echo; echo "--- $1 ---"; }

echo "=============================================="
echo " VPS Security Validation"
echo " Server: $SERVER_IP"
echo "=============================================="

# ---------------------------------------------------------------------------
section "SSH Configuration"

result=$(ssh $SSH_OPTS "$SSH_USER@$SERVER_IP" "sudo sshd -T 2>/dev/null | grep -E '^(passwordauthentication|pubkeyauthentication|permitrootlogin)'")

if echo "$result" | grep -q "passwordauthentication no"; then
    pass "Password authentication disabled"
else
    fail "Password authentication is NOT disabled (expected: no)"
fi

if echo "$result" | grep -q "pubkeyauthentication yes"; then
    pass "Public key authentication enabled"
else
    fail "Public key authentication is NOT enabled"
fi

if echo "$result" | grep -qE "permitrootlogin (prohibit-password|without-password)"; then
    pass "Root login restricted (key only)"
else
    fail "Root login is NOT restricted — check PermitRootLogin in sshd_config"
fi

# ---------------------------------------------------------------------------
section "Firewall"

ufw_output=$(ssh $SSH_OPTS "$SSH_USER@$SERVER_IP" "sudo ufw status verbose 2>/dev/null")

if echo "$ufw_output" | grep -q "Status: active"; then
    pass "UFW firewall is active"
else
    fail "UFW firewall is NOT active"
fi

if echo "$ufw_output" | grep -q "22/tcp.*ALLOW IN"; then
    pass "Port 22 (SSH) is open"
else
    fail "Port 22 is not explicitly allowed — you may lose SSH access"
fi

if echo "$ufw_output" | grep -qE "8081.*ALLOW IN"; then
    fail "Port 8081 is publicly open — dashboard is exposed!"
else
    pass "Port 8081 is NOT publicly exposed"
fi

if echo "$ufw_output" | grep -q "Default: deny (incoming)"; then
    pass "Default incoming policy is deny"
else
    fail "Default incoming policy is NOT deny"
fi

# ---------------------------------------------------------------------------
section "Dashboard Port Binding"

port_output=$(ssh $SSH_OPTS "$SSH_USER@$SERVER_IP" "sudo ss -tlnp 2>/dev/null | grep 8081 || echo 'not listening'")

if echo "$port_output" | grep -q "127.0.0.1:8081"; then
    pass "Port 8081 bound to 127.0.0.1 only (not public)"
elif echo "$port_output" | grep -q "not listening"; then
    fail "Port 8081 is not listening — bot may not be running"
else
    fail "Port 8081 is bound to 0.0.0.0 — dashboard is publicly accessible!"
fi

# ---------------------------------------------------------------------------
section ".env File"

env_perms=$(ssh $SSH_OPTS "$SSH_USER@$SERVER_IP" "stat -c '%a %U' $APP_DIR/.env 2>/dev/null || echo 'missing'")

if echo "$env_perms" | grep -q "missing"; then
    fail ".env file not found at $APP_DIR/.env"
elif echo "$env_perms" | grep -q "^600 $SSH_USER"; then
    pass ".env permissions are 600 and owned by $SSH_USER"
elif echo "$env_perms" | grep -q "^600"; then
    pass ".env permissions are 600"
else
    fail ".env permissions are too open: $env_perms (expected 600)"
fi

proxy_val=$(ssh $SSH_OPTS "$SSH_USER@$SERVER_IP" "grep -i '^PROXY_URL' $APP_DIR/.env 2>/dev/null || echo 'not set'")

if echo "$proxy_val" | grep -qE "^PROXY_URL=\s*$|not set"; then
    pass "PROXY_URL is blank (VPN proxy not active)"
else
    fail "PROXY_URL is set to: $proxy_val — may cause connection errors"
fi

# ---------------------------------------------------------------------------
section "Git — .env not tracked"

git_status=$(ssh $SSH_OPTS "$SSH_USER@$SERVER_IP" "cd $APP_DIR && git status 2>/dev/null")

if echo "$git_status" | grep -q ".env"; then
    fail ".env appears in git status — risk of it being committed!"
else
    pass ".env is not tracked by git"
fi

# ---------------------------------------------------------------------------
section "Clock Sync"

time_output=$(ssh $SSH_OPTS "$SSH_USER@$SERVER_IP" "timedatectl status 2>/dev/null")

if echo "$time_output" | grep -q "System clock synchronized: yes"; then
    pass "System clock is synchronized (NTP active)"
else
    fail "System clock is NOT synchronized — may cause invalid signature errors on CLOB API"
fi

# ---------------------------------------------------------------------------
section "Docker Container"

container_status=$(ssh $SSH_OPTS "$SSH_USER@$SERVER_IP" "docker compose -f $APP_DIR/docker-compose.yml ps 2>/dev/null || echo 'error'")

if echo "$container_status" | grep -q "running"; then
    pass "Bot container is running"
else
    fail "Bot container is NOT running"
fi

# ---------------------------------------------------------------------------
echo
echo "=============================================="
echo " Results: $PASS passed, $FAIL failed"
if [ "$FAIL" -eq 0 ]; then
    echo " All security checks passed."
else
    echo " $FAIL check(s) need attention — review FAIL items above."
fi
echo "=============================================="
