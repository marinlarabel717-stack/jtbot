# jtbot

JTBot is a Telegram keyword monitoring bot built in Go, with a clean, maintainable core pipeline and an inline-button admin panel.

## Features

The current version provides:

- Telegram Bot API long polling
- keyword matching
- match record persistence
- cooldown-based repeat protection
- DM queue and sender worker
- inline-button admin panel inside the bot chat

Current limitation:

- it uses Telegram Bot API, so direct messages only work if the target user has already opened a chat with the bot

## Project Structure

```text
cmd/jtbot/main.go
internal/app
internal/config
internal/listener
internal/logx
internal/matcher
internal/model
internal/queue
internal/rules
internal/sender
internal/service
internal/storage
pkg/tg
```

## Run

1. Copy `.env.example` to `.env`
2. Fill in `BOT_TOKEN` and `ADMIN_USER_ID`
3. Optionally fill in `MONITOR_CHAT_IDS` and `ALERT_CHAT_ID`
4. Adjust `configs/keywords.example.json`
5. Run `go run ./cmd/jtbot`
6. Open a private chat with the bot and send `/start`

## Admin Panel

The inline-button admin panel currently supports:

- view running status
- add keywords
- remove keywords
- enable or disable monitoring
- toggle dry-run mode
- change cooldown minutes
- change the DM template
- add monitored chat IDs
- remove monitored chat IDs
- set the current chat as the alert chat

## Notes

- runtime data such as `.env`, `data/`, session files, and exports are git-ignored
- deploy the bot with `go run ./cmd/jtbot` or a binary built from `./cmd/jtbot`
