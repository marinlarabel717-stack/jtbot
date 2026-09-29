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

func (c Chat) MessageLink(messageID int64) string {
	if messageID <= 0 {
		return ""
	}
	if username := strings.TrimSpace(strings.TrimPrefix(c.Username, "@")); username != "" {
		return "https://t.me/" + username + "/" + strconv.FormatInt(messageID, 10)
	}
	return c.Link(messageID)
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

func (u User) DialogLink() string {
	if username := strings.TrimSpace(strings.TrimPrefix(u.Username, "@")); username != "" {
		return "https://t.me/" + username
	}
	if u.ID > 0 {
		return "tg://user?id=" + strconv.FormatInt(u.ID, 10)
	}
	return ""
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

type KeywordRule struct {
	Text string `json:"text"`
	Mode string `json:"mode,omitempty"`
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
	UserID     int64     `json:"user_id"`
	Username   string    `json:"username"`
	ChatID     int64     `json:"chat_id"`
	Keywords   []string  `json:"keywords"`
	SourceText string    `json:"source_text,omitempty"`
	Message    string    `json:"message"`
	Sender     string    `json:"sender,omitempty"`
	Status     string    `json:"status"`
	Error      string    `json:"error,omitempty"`
	SentAt     time.Time `json:"sent_at"`
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

const (
	DMTemplateModeText          = "text"
	DMTemplateModePostBot       = "postbot"
	DMTemplateModeForward       = "forward"
	DMTemplateModeForwardHidden = "forward_hidden"
	DMTemplateModeQuickReply    = "quick_reply"
)

type DMTemplatePayload struct {
	Mode        string
	Message     string
	PostBotBot  string
	PostBotCode string
	SourceLink  string
	ShortcutID  int
	Raw         string
}

func EncodeTextDMTemplate(text string) string {
	return DMTemplateModeText + "::" + strings.TrimSpace(text)
}

func EncodePostBotDMTemplate(code string) string {
	return DMTemplateModePostBot + "::PostBot::" + strings.TrimSpace(code)
}

func EncodeForwardDMTemplate(link string) string {
	return DMTemplateModeForward + "::" + strings.TrimSpace(link)
}

func EncodeHiddenForwardDMTemplate(link string) string {
	return DMTemplateModeForwardHidden + "::" + strings.TrimSpace(link)
}

func EncodeQuickReplyDMTemplate(shortcutID int) string {
	return DMTemplateModeQuickReply + "::" + strconv.Itoa(shortcutID)
}

func ParseDMTemplate(raw string) DMTemplatePayload {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return DMTemplatePayload{Mode: DMTemplateModeText, Raw: raw}
	}

	parts := strings.Split(raw, "::")
	mode := strings.TrimSpace(parts[0])
	switch mode {
	case DMTemplateModeText:
		return DMTemplatePayload{
			Mode:    DMTemplateModeText,
			Message: strings.Join(parts[1:], "::"),
			Raw:     raw,
		}
	case DMTemplateModePostBot:
		payload := DMTemplatePayload{
			Mode:       DMTemplateModePostBot,
			PostBotBot: "PostBot",
			Raw:        raw,
		}
		if len(parts) >= 2 {
			payload.PostBotBot = strings.TrimSpace(parts[1])
		}
		if len(parts) >= 3 {
			payload.PostBotCode = strings.Join(parts[2:], "::")
		}
		return payload
	case DMTemplateModeForward:
		return DMTemplatePayload{
			Mode:       DMTemplateModeForward,
			SourceLink: strings.Join(parts[1:], "::"),
			Raw:        raw,
		}
	case DMTemplateModeForwardHidden:
		return DMTemplatePayload{
			Mode:       DMTemplateModeForwardHidden,
			SourceLink: strings.Join(parts[1:], "::"),
			Raw:        raw,
		}
	case DMTemplateModeQuickReply:
		payload := DMTemplatePayload{
			Mode: DMTemplateModeQuickReply,
			Raw:  raw,
		}
		if len(parts) >= 2 {
			payload.ShortcutID, _ = strconv.Atoi(strings.TrimSpace(parts[1]))
		}
		return payload
	default:
		return DMTemplatePayload{
			Mode:    DMTemplateModeText,
			Message: raw,
			Raw:     raw,
		}
	}
}

func (p DMTemplatePayload) Render(job DMJob) DMTemplatePayload {
	if p.Mode != DMTemplateModeText {
		return p
	}

	replacer := strings.NewReplacer(
		"{username}", dmSafeTemplateValue(job.Username, "friend"),
		"{chat_title}", dmSafeTemplateValue(job.ChatTitle, "group"),
		"{keywords}", strings.Join(job.Keywords, ", "),
		"{message}", job.SourceText,
	)
	p.Message = replacer.Replace(strings.TrimSpace(p.Message))
	if p.Raw == "" {
		p.Raw = EncodeTextDMTemplate(p.Message)
	}
	return p
}

func (p DMTemplatePayload) Summary() string {
	switch p.Mode {
	case DMTemplateModePostBot:
		return "内联Bot @PostBot | " + dmSafeTemplateValue(p.PostBotCode, "未填写代码")
	case DMTemplateModeForward:
		return "频道贴文转发 | " + dmSafeTemplateValue(p.SourceLink, "未填写链接")
	case DMTemplateModeForwardHidden:
		return "隐藏转发来源 | " + dmSafeTemplateValue(p.SourceLink, "未填写链接")
	case DMTemplateModeQuickReply:
		if p.ShortcutID > 0 {
			return "企业快捷回复 | 快捷回复 ID " + strconv.Itoa(p.ShortcutID)
		}
		return "企业快捷回复 | 未填写快捷回复 ID"
	default:
		return "文本直发 | " + dmTrimPreview(p.Message, 30)
	}
}

func dmSafeTemplateValue(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

func dmTrimPreview(value string, limit int) string {
	value = strings.TrimSpace(value)
	if len([]rune(value)) <= limit || limit <= 0 {
		return value
	}
	runes := []rune(value)
	return string(runes[:limit]) + "..."
}
