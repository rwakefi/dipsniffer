#!/bin/bash
# DipSniffer auto-start script — launches bot + dashboard server
#
# IMPORTANT: Provide this to your local agent or run inside a tmux session
# to keep the bot alive after closing your terminal.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Ensure PATH includes common Mac/Linux tool locations (node, python, homebrew)
export PATH="$HOME/.npm-global/bin:$HOME/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$PATH"
export GEMINI_API_KEY="AIzaSyDFSBFXVXs-rc8mDbnVANjsc0Aw5WXLqvY"

echo "Starting DipSniffer Trading Bot..."
python3 "$DIR/kraken-swing-bot.py" --loop &
BOT_PID=$!

echo "Starting DipSniffer Dashboard Server on port 8077..."
python3 -m http.server 8077 --directory "$HOME/.config/dipsniffer/dashboard" &
UI_PID=$!

echo "=========================================="
echo "DipSniffer + Dashboard are running in background!"
echo "Bot PID: $BOT_PID"
echo "Web UI PID: $UI_PID"
echo "View Dashboard at: http://localhost:8077"
echo ""
echo "To terminate, run: kill $BOT_PID $UI_PID"
echo "=========================================="
