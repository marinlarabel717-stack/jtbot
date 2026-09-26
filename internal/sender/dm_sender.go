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

type Sender struct {
	client      *tg.Client
	recordStore *storage.RecordStore
	template    string
	dryRun      bool
	logger      *logx.Logger
}

func New(client *tg.Client, recordStore *storage.RecordStore, template string, dryRun bool, logger *logx.Logger) *Sender {
	return &Sender{
		client:      client,
		recordStore: recordStore,
		template:    template,
		dryRun:      dryRun,
		logger:      logger,
	}
}

func (s *Sender) Run(ctx context.Context, jobs <-chan model.DMJob) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-jobs:
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

	if s.dryRun {
		record.Status = "dry_run"
		s.recordStore.SaveDMRecord(record)
		s.logger.Infof("dry-run dm user=%d keywords=%s", job.TargetUserID, strings.Join(job.Keywords, ","))
		return
	}

	if err := s.client.SendMessage(ctx, job.TargetUserID, text); err != nil {
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
		"{username}", safeValue(job.Username, "朋友"),
		"{chat_title}", safeValue(job.ChatTitle, "群组"),
		"{keywords}", strings.Join(job.Keywords, ", "),
		"{message}", job.SourceText,
	)
	return replacer.Replace(s.template)
}

func safeValue(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
