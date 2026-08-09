#!/bin/bash
# Setup systemd services for VWAP bots
# Run with sudo

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Installing systemd services..."

for asset in near sui doge; do
    cp "$SCRIPT_DIR/vwap-$asset.service" /etc/systemd/system/
    echo "  Installed vwap-$asset.service"
done

systemctl daemon-reload

echo ""
echo "Starting services..."

for asset in near sui doge; do
    systemctl enable vwap-$asset
    systemctl start vwap-$asset
    echo "  Started vwap-$asset"
done

echo ""
echo "Status:"
for asset in near sui doge; do
    status=$(systemctl is-active vwap-$asset)
    echo "  vwap-$asset: $status"
done

echo ""
echo "View logs with: journalctl -u vwap-near -f"
