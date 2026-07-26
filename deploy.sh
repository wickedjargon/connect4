#!/usr/bin/env bash
# deploy.sh — deploy connect4 to your Vultr server
# Usage: ./deploy.sh <ssh-host>
# Example: ./deploy.sh deploy@45.63.7.246
#
# Builds ON the server: this network can't reliably upload more than ~16KB in
# one stream (lossy upstream path), so we ship the ~13KB source tarball and
# compile remotely (Debian 13's cargo, ~80s on the 1-vCPU box).

set -euo pipefail

HOST="${1:?Usage: ./deploy.sh <ssh-host>}"
SSH="ssh -o IPQoS=throughput -o BatchMode=yes -o ConnectTimeout=10"

echo "==> Packing source..."
TGZ=$(mktemp)
trap 'rm -f "$TGZ"' EXIT
tar czf "$TGZ" Cargo.toml Cargo.lock src index.html favicon.ico \
    connect4.service connect4.fftp.io.nginx

echo "==> Uploading source ($(stat -c%s "$TGZ") bytes)..."
n=0
until timeout 30 $SSH "$HOST" 'cat > /tmp/c4src.tgz' < "$TGZ"; do
    n=$((n + 1))
    [ "$n" -ge 15 ] && { echo "✗ upload keeps stalling, giving up"; exit 1; }
    echo "  stalled, retry $n..."
done
LOCAL_MD5=$(md5sum "$TGZ" | cut -d' ' -f1)
REMOTE_MD5=$($SSH "$HOST" 'md5sum /tmp/c4src.tgz' | cut -d' ' -f1)
[ "$LOCAL_MD5" = "$REMOTE_MD5" ] || { echo "✗ upload corrupted"; exit 1; }

echo "==> Building and installing on server..."
$SSH "$HOST" sudo bash -s <<'REMOTE'
set -euo pipefail

command -v cargo >/dev/null || apt-get install -y -qq cargo

rm -rf /tmp/c4build && mkdir /tmp/c4build
tar xzf /tmp/c4src.tgz -C /tmp/c4build
cd /tmp/c4build
# 1 vCPU / 1GB RAM: single codegen job to avoid OOM
CARGO_BUILD_JOBS=1 cargo build --release 2>&1 | tail -2

mkdir -p /opt/connect4
install -m755 target/release/connect4 /opt/connect4/connect4

install -m644 connect4.service /etc/systemd/system/connect4.service
systemctl daemon-reload
systemctl enable connect4
systemctl restart connect4
echo "✓ connect4 service restarted"

if [ ! -f /etc/nginx/sites-available/connect4.fftp.io ]; then
    install -m644 connect4.fftp.io.nginx /etc/nginx/sites-available/connect4.fftp.io
    ln -sf /etc/nginx/sites-available/connect4.fftp.io /etc/nginx/sites-enabled/connect4.fftp.io
    nginx -t && systemctl reload nginx
    echo "✓ nginx site installed"
    if command -v certbot >/dev/null; then
        certbot --nginx -d connect4.fftp.io --non-interactive --agree-tos --redirect
        echo "✓ SSL configured"
    fi
else
    echo "✓ nginx site already configured (certbot manages SSL edits)"
fi

rm -rf /tmp/c4build /tmp/c4src.tgz
echo "==> Done!"
REMOTE

echo "==> Deploy complete: https://connect4.fftp.io"
