# DipSniffer: Mac Antigravity Auto-Setup Guide

Hello, Antigravity Agent! Your human user wants you to set up the DipSniffer trading bot on their macOS machine. Follow this strictly deterministic checklist to initialize the environment.

## 1. System Dependencies
Macs require explicit package management.
1. Use your terminal tools to verify `python3` and `pip3` exist.
2. Install the required Python libraries:
   `pip3 install ccxt pandas google-genai`
3. The bot utilizes Gemini Flash through the official Python SDK, not the older Gemini CLI path.

## 2. Environment & Auth Initialization
DipSniffer requires two sets of API keys to function: Kraken (for execution) and Gemini (for analysis).

1. **Ask the user** for their Kraken API Key and Kraken API Secret.
2. Create the configuration directory: `mkdir -p ~/.config/kraken`
3. Write the keys to `~/.config/kraken/config.toml` securely (permissions 600):
   ```toml
   api_key = "their_key_here"
   api_secret = "their_secret_here"
   ```
4. **Ask the user** for their Google AI / Gemini API key. Add it to their shell profile (e.g. `~/.zshrc` or `~/.bash_profile`) as `export GEMINI_API_KEY="their_key_here"`, or place it in a local secrets file that exports the same variable before launch.

## 3. Initializing the Strategy
1. The default strategy configuration is bundled in the repo.
2. Copy `strategy_config.json` from this repository to `~/.config/kraken/strategy_config.json`.
3. This file is intended to be the live baseline and may include newer safety controls such as squeeze-entry headroom limits and anti-churn tuning.

## 4. Boot Execution
1. The user can start the bot and its dashboard using the wrapper script.
2. Ensure the wrapper is executable: `chmod +x start-dipsniffer.sh`
3. You can execute it for them: `./start-dipsniffer.sh`
4. Provide the user with the localhost URL (`http://localhost:8077`) so they can view the dashboard.
