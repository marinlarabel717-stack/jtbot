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
	matcher     *matcher.KeywordMatcher
	rules       *rules.Engine
	queue       *queue.MessageQueue
	recordStore *storage.RecordStore
	client      *tg.Client
	alertChatID int64
	logger      *logx.Logger
}

func NewTriggerService(
	matcher *matcher.KeywordMatcher,
	rules *rules.Engine,
	queue *queue.MessageQueue,
	recordStore *storage.RecordStore,
	client *tg.Client,
	alertChatID int64,
	logger *logx.Logger,
) *TriggerService {
	return &TriggerService{
		matcher:     matcher,
		rules:       rules,
		queue:       queue,
		recordStore: recordStore,
		client:      client,
		alertChatID: alertChatID,
		logger:      logger,
	}
}

func (s *TriggerService) HandleUpdate(ctx context.Context, update model.Update) error {
	msg := update.Message
	if msg.MessageID == 0 || msg.From == nil || msg.From.IsBot {
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

	keywords := s.matcher.Match(content)
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
			ChatID:    msg.Chat.ID,
			ChatTitle: chatTitle,
			Keyword:   keyword,
			Message:   content,
			MatchedAt: time.Now(),
			UpdateID:  update.UpdateID,
			MessageID: msg.MessageID,
		})
	}

	s.logger.Infof("matched user=%d chat=%d keywords=%s", msg.From.ID, msg.Chat.ID, strings.Join(keywords, ","))

	if s.alertChatID != 0 {
		alertText := fmt.Sprintf(
			"关键词命中\n群: %s\n用户: @%s (%d)\n关键词: %s\n消息: %s",
			chatTitle,
			safeUsername(msg.From.Username),
			msg.From.ID,
			strings.Join(keywords, ", "),
			content,
		)
		if err := s.client.SendMessage(ctx, s.alertChatID, alertText); err != nil {
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
