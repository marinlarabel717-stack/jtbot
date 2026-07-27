# jtbot

JTBot is a Telegram multi-account keyword monitoring bot with an admin control panel and a DM account pool.

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
