package model

import "time"

type Update struct {
	UpdateID int     `json:"update_id"`
	Message  Message `json:"message"`
}

type Message struct {
	MessageID int64    `json:"message_id"`
	From      *User    `json:"from"`
	Chat      Chat     `json:"chat"`
	Text      string   `json:"text"`
	Caption   string   `json:"caption"`
	Date      int64    `json:"date"`
	Entities  []Entity `json:"entities"`
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

type User struct {
	ID           int64  `json:"id"`
	IsBot        bool   `json:"is_bot"`
	FirstName    string `json:"first_name"`
	LastName     string `json:"last_name"`
	Username     string `json:"username"`
	LanguageCode string `json:"language_code"`
}

type Entity struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
	Length int    `json:"length"`
}

type MatchRecord struct {
	UserID    int64     `json:"user_id"`
	Username  string    `json:"username"`
	ChatID    int64     `json:"chat_id"`
	ChatTitle string    `json:"chat_title"`
	Keyword   string    `json:"keyword"`
	Message   string    `json:"message"`
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
	Status   string    `json:"status"`
	Error    string    `json:"error,omitempty"`
	SentAt   time.Time `json:"sent_at"`
}

type DMJob struct {
	TargetUserID int64
	Username     string
	ChatID       int64
	ChatTitle    string
	Keywords     []string
	SourceText   string
	TriggeredAt  time.Time
}
