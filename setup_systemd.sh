#!/bin/bash
#
# Setup systemd service for VWAP Bot
#

SERVICE_FILE="/etc/systemd/system/vwap-bot.service"
BOT_DIR="/home/clawdbot/clawd/projects/kalshi-vwap-reversal"

echo "Installing VWAP Bot systemd service..."

# Copy service file
sudo cp "$BOT_DIR/vwap-bot.service" "$SERVICE_FILE"

# Reload systemd
sudo systemctl daemon-reload

# Enable service (start on boot)
sudo systemctl enable vwap-bot

echo ""
echo "✅ Service installed!"
echo ""
echo "Commands:"
echo "  sudo systemctl start vwap-bot    # Start the bot"
echo "  sudo systemctl stop vwap-bot     # Stop the bot"
echo "  sudo systemctl restart vwap-bot  # Restart the bot"
echo "  sudo systemctl status vwap-bot   # Check status"
echo "  journalctl -u vwap-bot -f        # Follow logs"
echo ""
echo "Note: Service will NOT start if circuit breaker is tripped."
echo "Reset with: ./reset_circuit_breaker.sh"
