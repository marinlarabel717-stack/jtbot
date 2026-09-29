package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/marinlarabel717-stack/jtbot/internal/logx"
	"github.com/marinlarabel717-stack/jtbot/internal/matcher"
	"github.com/marinlarabel717-stack/jtbot/internal/model"
	"github.com/marinlarabel717-stack/jtbot/internal/queue"
	"github.com/marinlarabel717-stack/jtbot/internal/rules"
	"github.com/marinlarabel717-stack/jtbot/internal/storage"
	"github.com/marinlarabel717-stack/jtbot/pkg/tg"
)

type TriggerService struct {
	matcher      *matcher.KeywordMatcher
	keywordStore *storage.KeywordStore
	rules        *rules.Engine
	queue        *queue.MessageQueue
	recordStore  *storage.RecordStore
	client       *tg.Client
	alertClient  *tg.Client
	settings     *storage.SettingsStore
	blacklist    *storage.BlacklistStore
	monitorLabel string
	logger       *logx.Logger
}

func NewTriggerService(
	matcher *matcher.KeywordMatcher,
	keywordStore *storage.KeywordStore,
	rules *rules.Engine,
	queue *queue.MessageQueue,
	recordStore *storage.RecordStore,
	client *tg.Client,
	alertClient *tg.Client,
	settings *storage.SettingsStore,
	blacklist *storage.BlacklistStore,
	monitorLabel string,
	logger *logx.Logger,
) *TriggerService {
	return &TriggerService{
		matcher:      matcher,
		keywordStore: keywordStore,
		rules:        rules,
		queue:        queue,
		recordStore:  recordStore,
		client:       client,
		alertClient:  alertClient,
		settings:     settings,
		blacklist:    blacklist,
		monitorLabel: monitorLabel,
		logger:       logger,
	}
}

func (s *TriggerService) HandleUpdate(ctx context.Context, update model.Update) error {
	msg := update.Message
	if msg == nil || msg.MessageID == 0 || msg.From == nil || msg.From.IsBot {
		return nil
	}

	if !s.rules.MarkProcessed(msg.Chat.ID, msg.MessageID) {
		return nil
	}

	if !s.rules.AllowChat(msg.Chat.ID) {
		return nil
	}

	content := strings.TrimSpace(msg.Content())
	if content == "" {
		return nil
	}

	if s.blacklist != nil {
		if s.blacklist.IsUserBlocked(msg.From.ID) || s.blacklist.IsChatBlocked(msg.Chat.ID) {
			return nil
		}
	}

	if maxLength := s.settings.MaxMessageLength(); maxLength > 0 && len([]rune(content)) > maxLength {
		return nil
	}
	if minLength := s.settings.MinMessageLength(); minLength > 0 && len([]rune(content)) < minLength {
		return nil
	}

	if s.settings.FilterNoUsername() && strings.TrimSpace(msg.From.Username) == "" {
		return nil
	}

	if s.settings.FilterNoAvatar() && !msg.From.HasAvatar {
		return nil
	}

	if minAgeDays := s.settings.MinAccountAgeDays(); minAgeDays > 0 && estimateAccountAgeDays(msg.From.ID) < minAgeDays {
		return nil
	}

	keywords := s.matcher.Match(content, s.keywordStore.List())
	if len(keywords) == 0 {
		return nil
	}

	chatTitle := msg.Chat.Title
	if chatTitle == "" {
		chatTitle = msg.Chat.DisplayTitle()
	}
	chatLink := msg.Chat.Link(msg.MessageID)
	messageLink := msg.Chat.MessageLink(msg.MessageID)
	userLink := msg.From.DialogLink()
	matchedAt := time.Now()

	for _, keyword := range keywords {
		s.recordStore.SaveMatchRecord(model.MatchRecord{
			UserID:    msg.From.ID,
			Username:  msg.From.Username,
			Name:      strings.TrimSpace(msg.From.FirstName + " " + msg.From.LastName),
			ChatID:    msg.Chat.ID,
			ChatTitle: chatTitle,
			Keyword:   keyword,
			Message:   content,
			Monitor:   s.monitorLabel,
			MatchedAt: matchedAt,
			UpdateID:  update.UpdateID,
			MessageID: msg.MessageID,
		})
	}

	s.logger.Infof("matched monitor=%s user=%d chat=%d keywords=%s", s.monitorLabel, msg.From.ID, msg.Chat.ID, strings.Join(keywords, ","))

	if alertChatID := s.settings.AlertChatID(); alertChatID != 0 {
		alertText := formatMatchAlertText(chatTitle, chatLink, msg.From, s.monitorLabel, keywords, matchedAt, content)
		alertClient := s.alertClient
		replyMarkup := (*model.InlineKeyboardMarkup)(nil)
		if alertClient != nil {
			replyMarkup = buildMatchAlertKeyboard(msg, messageLink, userLink)
		} else {
			alertClient = s.client
		}
		if err := alertClient.SendMessage(ctx, alertChatID, alertText, replyMarkup); err != nil {
			s.logger.Errorf("send alert failed: %v", err)
		}
	}

	job := model.DMJob{
		TargetUserID: msg.From.ID,
		TargetLabel:  formatAlertUserLabel(msg.From),
		Username:     msg.From.Username,
		ChatID:       msg.Chat.ID,
		ChatTitle:    chatTitle,
		ChatLink:     chatLink,
		Keywords:     keywords,
		SourceText:   content,
		TriggeredAt:  matchedAt,
	}
	if !s.rules.CanQueueDM(job) {
		s.logger.Infof("user=%d filtered by dm cooldown rules, skip dm", msg.From.ID)
		return nil
	}

	return s.queue.Publish(ctx, job)
}

func safeUsername(username string) string {
	if strings.TrimSpace(username) == "" {
		return "no_username"
	}
	return username
}

func formatUserLabel(user *model.User) string {
	if user == nil {
		return "unknown"
	}
	if username := strings.TrimSpace(user.Username); username != "" {
		return "@" + username
	}
	if name := strings.TrimSpace(user.FirstName + " " + user.LastName); name != "" {
		return name
	}
	return "no_username"
}

func formatAlertUserLabel(user *model.User) string {
	if user == nil {
		return "unknown"
	}

	name := strings.TrimSpace(user.FirstName + " " + user.LastName)
	username := strings.TrimSpace(user.Username)
	switch {
	case name != "" && username != "":
		return fmt.Sprintf("%s (@%s)", name, username)
	case username != "":
		return "@" + username
	case name != "":
		return name
	default:
		return "no_username"
	}
}

func formatMatchAlertText(chatTitle, chatLink string, user *model.User, monitorLabel string, keywords []string, matchedAt time.Time, content string) string {
	lines := []string{
		"🔔 关键词触发提醒",
		"",
		fmt.Sprintf("📍 来源群组: %s", safeValue(chatTitle, "unknown")),
		fmt.Sprintf("🔗 群组链接: %s", displayLink(chatLink)),
		fmt.Sprintf("👤 发送用户: %s", formatAlertUserLabel(user)),
		fmt.Sprintf("🆔 用户ID: %d", userID(user)),
		fmt.Sprintf("🔑 触发关键词: %s", strings.Join(keywords, ", ")),
		fmt.Sprintf("📱 监控账号: %s", safeValue(monitorLabel, "unknown")),
		fmt.Sprintf("⏰ 时间: %s", matchedAt.Format("2006-01-02 15:04:05")),
		"",
		"📝 消息内容:",
		content,
	}
	return strings.Join(lines, "\n")
}

func buildMatchAlertKeyboard(msg *model.Message, messageLink, userLink string) *model.InlineKeyboardMarkup {
	rows := make([][]model.InlineKeyboardButton, 0, 3)
	actionRow := make([]model.InlineKeyboardButton, 0, 2)
	if strings.TrimSpace(messageLink) != "" {
		actionRow = append(actionRow, model.InlineKeyboardButton{Text: "🚀 直达消息", URL: messageLink})
	}
	if strings.TrimSpace(userLink) != "" {
		actionRow = append(actionRow, model.InlineKeyboardButton{Text: "💬 一键私信", URL: userLink})
	}
	if len(actionRow) > 0 {
		rows = append(rows, actionRow)
	}
	rows = append(rows,
		[]model.InlineKeyboardButton{
			{Text: "🚫 屏蔽用户", CallbackData: fmt.Sprintf("admin:blacklist:user:%d:%s", msg.From.ID, safeUsername(msg.From.Username))},
			{Text: "🚫 屏蔽此群", CallbackData: fmt.Sprintf("admin:blacklist:chat:%d", msg.Chat.ID)},
		},
	)
	return &model.InlineKeyboardMarkup{InlineKeyboard: rows}
}

func safeValue(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func displayLink(link string) string {
	link = strings.TrimSpace(link)
	if link == "" {
		return "暂无公开链接"
	}
	link = strings.TrimPrefix(link, "https://")
	link = strings.TrimPrefix(link, "http://")
	return link
}

func userID(user *model.User) int64 {
	if user == nil {
		return 0
	}
	return user.ID
}

func estimateAccountAgeDays(userID int64) int {
	switch {
	case userID < 1_000_000_000:
		return 365 * 5
	case userID < 2_000_000_000:
		return 365 * 2
	case userID < 5_000_000_000:
		return 180
	default:
		return 30
	}
}
