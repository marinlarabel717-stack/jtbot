# jtbot

JTBot is a Telegram keyword monitoring tool built in Go for Telegram user accounts. It logs in with a real account session, listens to the groups that account has already joined, matches keywords, and can DM matched users.

## Features

The current version provides:

- Telegram user session login via MTProto
- listening to group and supergroup messages seen by that account
- keyword matching
- match record persistence
- cooldown-based repeat protection
- DM queue and sender worker
- JSON-based runtime settings for monitored chat IDs and template

Current limitation:

- first login requires interactive code entry in the terminal
- monitored channels/groups must already be joined by the account session

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
2. Fill in `APP_ID`, `APP_HASH`, and `PHONE`
3. Optionally fill in `MONITOR_CHAT_IDS` and `ALERT_CHAT_ID`
4. Adjust `configs/keywords.example.json`
5. Run `go run ./cmd/jtbot`
6. Enter the Telegram login code the first time the session is created
7. Keep the account in the groups you want to monitor

## Notes

- runtime data such as `.env`, `data/`, and session files are git-ignored
- deploy the bot with `go run ./cmd/jtbot` or a binary built from `./cmd/jtbot`
- this version is designed for user-account monitoring, not Bot API group bots
