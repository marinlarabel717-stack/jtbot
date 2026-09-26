# jtbot

JTBot is a Telegram multi-account keyword monitoring bot with an admin control panel and a DM account pool.

## Go First Version

This repository now also contains a Go first version focused on the core end-to-end flow:

- listen to group messages with Telegram Bot API long polling
- match configured keywords
- store match records
- apply user cooldown
- enqueue DM jobs
- send direct messages to matched users

Current Go scope is intentionally small and stable-first. It does not yet replace the original Python Telethon multi-session account pool. The first version is meant to get the main pipeline running cleanly in Go.

### Go Structure

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

### Go Run

1. Copy `.env.example` to `.env`
2. Fill in `BOT_TOKEN`, `MONITOR_CHAT_IDS`, and optionally `ALERT_CHAT_ID`
3. Adjust `configs/keywords.example.json`
4. Run `go run ./cmd/jtbot`

### Important Limitation

The Go first version uses Telegram Bot API, so direct messages only work if the target user has already started the bot or otherwise opened a chat with it before. The older Python project uses user-account sessions, which is why its DM capability is broader.

## Features

- Multi-account Telegram monitoring with Telethon sessions
- Keyword management and keyword-triggered alert forwarding
- User filtering by cooldown, message length, estimated account age, username, avatar, and Telegram Premium status
- User/chat blacklist management
- Export matched records by time range, keyword, or full CSV
- DM account pool with session upload, status checks, and daily send limits
- DM templates for plain text, PostBot inline content, channel forwarding, and hidden-source forwarding
- Optional sticker-first greeting flow and DM send records

## Files

- `jtbot.py`: main bot program
- `requirements.txt`: Python dependencies
- `proxy.txt`: optional proxy configuration
- `.env.example`: environment variable template

## Quick Start

1. Create a virtual environment.
2. Install dependencies from `requirements.txt`.
3. Copy `.env.example` to `.env` and fill in your values.
4. Run `python jtbot.py`.

## Notes

- This public repository does not include the real `.env` file.
- Runtime data such as sessions, exports, logs, and generated config files are git-ignored.
