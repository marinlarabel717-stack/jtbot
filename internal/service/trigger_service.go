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
		chatTitle = msg.Chat.Username
	}
	if chatTitle == "" {
		chatTitle = "unknown"
	}

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
			MatchedAt: time.Now(),
			UpdateID:  update.UpdateID,
			MessageID: msg.MessageID,
		})
	}

	s.logger.Infof("matched monitor=%s user=%d chat=%d keywords=%s", s.monitorLabel, msg.From.ID, msg.Chat.ID, strings.Join(keywords, ","))

	if alertChatID := s.settings.AlertChatID(); alertChatID != 0 {
		alertText := fmt.Sprintf(
			"关键词命中\n监控号: %s\n群: %s\n用户: %s (%d)\n关键词: %s\n消息: %s",
			s.monitorLabel,
			chatTitle,
			formatUserLabel(msg.From),
			msg.From.ID,
			strings.Join(keywords, ", "),
			content,
		)
		alertClient := s.alertClient
		replyMarkup := (*model.InlineKeyboardMarkup)(nil)
		if alertClient != nil {
			replyMarkup = &model.InlineKeyboardMarkup{
				InlineKeyboard: [][]model.InlineKeyboardButton{
					{
						{Text: "拉黑用户", CallbackData: fmt.Sprintf("admin:blacklist:user:%d:%s", msg.From.ID, safeUsername(msg.From.Username))},
						{Text: "拉黑群", CallbackData: fmt.Sprintf("admin:blacklist:chat:%d", msg.Chat.ID)},
					},
				},
			}
		} else {
			alertClient = s.client
		}
		if err := alertClient.SendMessage(ctx, alertChatID, alertText, replyMarkup); err != nil {
			s.logger.Errorf("send alert failed: %v", err)
		}
	}

	if !s.rules.CanQueueDM(msg.From.ID) {
		s.logger.Infof("user=%d in cooldown, skip dm", msg.From.ID)
		return nil
	}

	return s.queue.Publish(ctx, model.DMJob{
		TargetUserID: msg.From.ID,
		Username:     msg.From.Username,
		ChatID:       msg.Chat.ID,
		ChatTitle:    chatTitle,
		Keywords:     keywords,
		SourceText:   content,
		TriggeredAt:  time.Now(),
	})
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
