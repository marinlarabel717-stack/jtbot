package sender

import (
	"context"
	"strings"
	"time"

	"github.com/marinlarabel717-stack/jtbot/internal/logx"
	"github.com/marinlarabel717-stack/jtbot/internal/model"
	"github.com/marinlarabel717-stack/jtbot/internal/storage"
	"github.com/marinlarabel717-stack/jtbot/pkg/tg"
)

type DMDispatcher interface {
	SendDM(ctx context.Context, fallback *tg.Client, job model.DMJob, text string) (string, error)
}

type Sender struct {
	client      *tg.Client
	dispatcher  DMDispatcher
	recordStore *storage.RecordStore
	settings    *storage.SettingsStore
	logger      *logx.Logger
}

func New(client *tg.Client, dispatcher DMDispatcher, recordStore *storage.RecordStore, settings *storage.SettingsStore, logger *logx.Logger) *Sender {
	return &Sender{
		client:      client,
		dispatcher:  dispatcher,
		recordStore: recordStore,
		settings:    settings,
		logger:      logger,
	}
}

func (s *Sender) Run(ctx context.Context, jobs <-chan model.DMJob) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}
			s.handleJob(ctx, job)
		}
	}
}

func (s *Sender) handleJob(ctx context.Context, job model.DMJob) {
	text := s.renderTemplate(job)
	record := model.DMRecord{
		UserID:   job.TargetUserID,
		Username: job.Username,
		ChatID:   job.ChatID,
		Keywords: append([]string(nil), job.Keywords...),
		Message:  text,
		SentAt:   time.Now(),
	}

	if s.settings.IsDryRun() {
		record.Sender = "dry_run"
		record.Status = "dry_run"
		s.recordStore.SaveDMRecord(record)
		s.logger.Infof("dry-run dm user=%d keywords=%s", job.TargetUserID, strings.Join(job.Keywords, ","))
		return
	}

	senderLabel := s.client.Label()
	var err error
	if s.dispatcher != nil {
		senderLabel, err = s.dispatcher.SendDM(ctx, s.client, job, text)
	} else {
		err = s.client.SendDirectMessage(ctx, job.TargetUserID, job.Username, text)
	}
	record.Sender = senderLabel

	if err != nil {
		record.Status = "failed"
		record.Error = err.Error()
		s.recordStore.SaveDMRecord(record)
		s.logger.Errorf("send dm failed user=%d err=%v", job.TargetUserID, err)
		return
	}

	record.Status = "sent"
	s.recordStore.SaveDMRecord(record)
	s.logger.Infof("dm sent user=%d keywords=%s", job.TargetUserID, strings.Join(job.Keywords, ","))
}

func (s *Sender) renderTemplate(job model.DMJob) string {
	replacer := strings.NewReplacer(
		"{username}", safeValue(job.Username, "friend"),
		"{chat_title}", safeValue(job.ChatTitle, "group"),
		"{keywords}", strings.Join(job.Keywords, ", "),
		"{message}", job.SourceText,
	)
	return replacer.Replace(s.settings.DMTemplate())
}

func safeValue(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
