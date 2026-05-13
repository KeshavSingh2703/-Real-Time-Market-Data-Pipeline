#!/bin/bash
# deploy.sh — run this on a fresh Ubuntu 22.04 VPS
set -e  # exit immediately on any error

echo "──────────────────────────────────────"
echo " Market Pipeline — Production Deploy"
echo "──────────────────────────────────────"

# ── 1. System update ──────────────────────────────────
echo "[1/6] Updating system packages..."
sudo apt-get update && sudo apt-get upgrade -y

# ── 2. Install Docker ─────────────────────────────────
echo "[2/6] Installing Docker..."
sudo apt-get install -y ca-certificates curl gnupg lsb-release

sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
    sudo gpg --dearmor --yes -o /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# ── 3. Docker group ───────────────────────────────────
echo "[3/6] Adding $USER to docker group..."
sudo usermod -aG docker $USER
# Note: group change takes effect in new shell sessions.
# We use 'sudo docker' below to avoid needing to re-login.

# ── 4. Clone project ──────────────────────────────────
echo "[4/6] Cloning project..."
# Replace with your actual repo URL before running
git clone https://github.com/yourusername/market-pipeline.git
cd market-pipeline

# ── 5. Environment file ───────────────────────────────
echo "[5/6] Checking for .env.prod..."
if [ ! -f .env.prod ]; then
    echo "ERROR: .env.prod not found."
    echo "Copy .env.prod.example to .env.prod and fill in real values."
    echo "  cp .env.prod.example .env.prod && nano .env.prod"
    exit 1
fi

# ── 6. Deploy ─────────────────────────────────────────
echo "[6/6] Starting services..."
sudo docker compose --env-file .env.prod -f docker-compose.prod.yml up -d --build

echo ""
echo "Running containers:"
sudo docker compose -f docker-compose.prod.yml ps

echo ""
echo "✓ Deploy complete."
echo "  Dashboard → http://$(curl -s ifconfig.me)"
echo "  API health → http://$(curl -s ifconfig.me)/api/health"
