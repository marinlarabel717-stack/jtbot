package app

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/marinlarabel717-stack/jtbot/internal/config"
	"github.com/marinlarabel717-stack/jtbot/internal/logx"
	"github.com/marinlarabel717-stack/jtbot/internal/matcher"
	"github.com/marinlarabel717-stack/jtbot/internal/queue"
	"github.com/marinlarabel717-stack/jtbot/internal/rules"
	"github.com/marinlarabel717-stack/jtbot/internal/sender"
	"github.com/marinlarabel717-stack/jtbot/internal/service"
	"github.com/marinlarabel717-stack/jtbot/internal/storage"
	"github.com/marinlarabel717-stack/jtbot/pkg/tg"
)

type App struct {
	cfg     config.Config
	logger  *logx.Logger
	client  *tg.Client
	trigger *service.TriggerService
	sender  *sender.Sender
	queue   *queue.MessageQueue
}

func New() (*App, error) {
	_ = config.LoadDotEnv(".env")

	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}

	logger := logx.New(cfg.LogLevel)

	if err := os.MkdirAll(filepath.Dir(cfg.RecordsFile), 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	keywordStore := storage.NewKeywordStore(cfg.KeywordsFile)
	keywords, err := keywordStore.Load()
	if err != nil {
		return nil, err
	}
	logger.Infof("loaded %d keywords", len(keywords))

	recordStore, err := storage.NewRecordStore(cfg.RecordsFile)
	if err != nil {
		return nil, err
	}

	settingsStore, err := storage.NewSettingsStore(cfg.SettingsFile, storage.RuntimeSettings{
		MonitoringEnabled: true,
		MonitorChatIDs:    mapKeys(cfg.MonitorChatIDs),
		AlertChatID:       cfg.AlertChatID,
		CooldownMinutes:   int(cfg.Cooldown / time.Minute),
		DMTemplate:        cfg.DMTemplate,
		DryRun:            cfg.DryRun,
	})
	if err != nil {
		return nil, err
	}

	client := tg.NewClient(cfg.AppID, cfg.AppHash, cfg.Phone, cfg.SessionFile, logger)
	jobQueue := queue.NewMessageQueue(cfg.QueueSize)
	m := matcher.NewKeywordMatcher()
	ruleEngine := rules.NewEngine(settingsStore, recordStore)
	dmSender := sender.New(client, recordStore, settingsStore, logger)
	triggerSvc := service.NewTriggerService(m, keywordStore, ruleEngine, jobQueue, recordStore, client, settingsStore, logger)

	return &App{
		cfg:     cfg,
		logger:  logger,
		client:  client,
		trigger: triggerSvc,
		sender:  dmSender,
		queue:   jobQueue,
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	a.logger.Infof("jtbot user session version started")
	go a.sender.Run(ctx, a.queue.Consume())
	return a.client.Run(ctx, a.trigger.HandleUpdate)
}

func mapKeys(input map[int64]struct{}) []int64 {
	result := make([]int64, 0, len(input))
	for id := range input {
		result = append(result, id)
	}
	return result
}
