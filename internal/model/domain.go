package model

import (
	"strconv"
	"strings"
	"time"
)

type Update struct {
	UpdateID      int            `json:"update_id"`
	Message       *Message       `json:"message,omitempty"`
	CallbackQuery *CallbackQuery `json:"callback_query,omitempty"`
}

type Message struct {
	MessageID int64     `json:"message_id"`
	From      *User     `json:"from"`
	Chat      Chat      `json:"chat"`
	Text      string    `json:"text"`
	Caption   string    `json:"caption"`
	Date      int64     `json:"date"`
	Entities  []Entity  `json:"entities"`
	Document  *Document `json:"document,omitempty"`
}

func (m Message) Content() string {
	if m.Text != "" {
		return m.Text
	}
	return m.Caption
}

type Chat struct {
	ID       int64  `json:"id"`
	Type     string `json:"type"`
	Title    string `json:"title"`
	Username string `json:"username"`
}

func (c Chat) DisplayTitle() string {
	if title := strings.TrimSpace(c.Title); title != "" {
		return title
	}
	if username := strings.TrimSpace(c.Username); username != "" {
		return username
	}
	return "unknown"
}

func (c Chat) Link(messageID int64) string {
	if username := strings.TrimSpace(strings.TrimPrefix(c.Username, "@")); username != "" {
		return "https://t.me/" + username
	}
	if messageID <= 0 {
		return ""
	}

	rawID := strconv.FormatInt(c.ID, 10)
	if strings.HasPrefix(rawID, "-100") {
		internalID := strings.TrimPrefix(rawID, "-100")
		if internalID != "" {
			return "https://t.me/c/" + internalID + "/" + strconv.FormatInt(messageID, 10)
		}
	}
	return ""
}

type User struct {
	ID           int64  `json:"id"`
	IsBot        bool   `json:"is_bot"`
	FirstName    string `json:"first_name"`
	LastName     string `json:"last_name"`
	Username     string `json:"username"`
	Phone        string `json:"phone,omitempty"`
	LanguageCode string `json:"language_code"`
	HasAvatar    bool   `json:"has_avatar"`
}

type Entity struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
	Length int    `json:"length"`
}

type CallbackQuery struct {
	ID      string   `json:"id"`
	From    *User    `json:"from"`
	Message *Message `json:"message,omitempty"`
	Data    string   `json:"data"`
}

type InlineKeyboardMarkup struct {
	InlineKeyboard [][]InlineKeyboardButton `json:"inline_keyboard"`
}

type InlineKeyboardButton struct {
	Text         string `json:"text"`
	CallbackData string `json:"callback_data,omitempty"`
	URL          string `json:"url,omitempty"`
}

type Document struct {
	FileID   string `json:"file_id"`
	FileName string `json:"file_name"`
	MimeType string `json:"mime_type"`
	FileSize int64  `json:"file_size"`
}

type MatchRecord struct {
	UserID    int64     `json:"user_id"`
	Username  string    `json:"username"`
	Name      string    `json:"name"`
	ChatID    int64     `json:"chat_id"`
	ChatTitle string    `json:"chat_title"`
	Keyword   string    `json:"keyword"`
	Message   string    `json:"message"`
	Monitor   string    `json:"monitor"`
	MatchedAt time.Time `json:"matched_at"`
	UpdateID  int       `json:"update_id"`
	MessageID int64     `json:"message_id"`
}

type DMRecord struct {
	UserID   int64     `json:"user_id"`
	Username string    `json:"username"`
	ChatID   int64     `json:"chat_id"`
	Keywords []string  `json:"keywords"`
	Message  string    `json:"message"`
	Sender   string    `json:"sender,omitempty"`
	Status   string    `json:"status"`
	Error    string    `json:"error,omitempty"`
	SentAt   time.Time `json:"sent_at"`
}

type DMJob struct {
	TargetUserID int64
	TargetLabel  string
	Username     string
	ChatID       int64
	ChatTitle    string
	ChatLink     string
	Keywords     []string
	SourceText   string
	TriggeredAt  time.Time
}
