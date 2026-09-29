# jtbot

JTBot is a Telegram keyword monitoring tool built in Go. It uses a real Telegram user session to join and monitor groups, and can optionally expose an admin control panel through a Bot API bot with inline buttons.

## Features

The current version provides:

- Telegram user session login via MTProto
- listening to group and supergroup messages seen by that account
- optional Bot API admin backend for `/start` and inline button controls
- keyword matching
- match record persistence
- cooldown-based repeat protection
- basic blacklist and sender/message filters
- DM queue and sender worker
- JSON-based runtime settings for monitored chat IDs and template

Current limitation:

- first login needs a Telegram code; if `BOT_TOKEN` + `ADMIN_USER_ID` are configured, you can now complete it from the admin backend, otherwise it falls back to terminal input
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
3. If you want the inline-button admin backend, also fill in `BOT_TOKEN` and `ADMIN_USER_ID`
4. Optionally fill in `MONITOR_CHAT_IDS` and `ALERT_CHAT_ID`
5. Adjust `configs/keywords.example.json`
6. Run `go run ./cmd/jtbot`
7. Complete the first Telegram login:
   - if admin backend is enabled, wait for the bot to prompt you in the admin chat and reply there with the login code
   - otherwise enter the login code in the terminal
8. Keep the account in the groups you want to monitor
9. If Bot API admin is enabled, open that bot and send `/start`

## Notes

- runtime data such as `.env`, `data/`, and session files are git-ignored
- deploy the bot with `go run ./cmd/jtbot` or a binary built from `./cmd/jtbot`
- monitoring is done by the user session, not by the bot account
- the admin bot is only for control panel actions and inline buttons
