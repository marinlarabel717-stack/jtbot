package app

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/marinlarabel717-stack/jtbot/internal/config"
	"github.com/marinlarabel717-stack/jtbot/internal/listener"
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
	cfg    config.Config
	logger *logx.Logger
	poller *listener.Poller
	sender *sender.Sender
	queue  *queue.MessageQueue
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

	client := tg.NewClient(cfg.BotToken)
	jobQueue := queue.NewMessageQueue(cfg.QueueSize)
	m := matcher.NewKeywordMatcher(keywords)
	ruleEngine := rules.NewEngine(cfg.MonitorChatIDs, cfg.Cooldown, recordStore)
	dmSender := sender.New(client, recordStore, cfg.DMTemplate, cfg.DryRun, logger)
	svc := service.NewTriggerService(m, ruleEngine, jobQueue, recordStore, client, cfg.AlertChatID, logger)
	poller := listener.NewPoller(client, cfg.PollTimeout, logger, svc.HandleUpdate)

	return &App{
		cfg:    cfg,
		logger: logger,
		poller: poller,
		sender: dmSender,
		queue:  jobQueue,
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	a.logger.Infof("jtbot go first version started")
	go a.sender.Run(ctx, a.queue.Consume())
	return a.poller.Run(ctx)
}
