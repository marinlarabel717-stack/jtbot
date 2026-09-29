package app

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/marinlarabel717-stack/jtbot/internal/config"
	"github.com/marinlarabel717-stack/jtbot/internal/listener"
	"github.com/marinlarabel717-stack/jtbot/internal/logx"
	"github.com/marinlarabel717-stack/jtbot/internal/matcher"
	"github.com/marinlarabel717-stack/jtbot/internal/model"
	"github.com/marinlarabel717-stack/jtbot/internal/queue"
	"github.com/marinlarabel717-stack/jtbot/internal/rules"
	"github.com/marinlarabel717-stack/jtbot/internal/sender"
	"github.com/marinlarabel717-stack/jtbot/internal/service"
	"github.com/marinlarabel717-stack/jtbot/internal/storage"
	"github.com/marinlarabel717-stack/jtbot/pkg/tg"
)

type App struct {
	cfg         config.Config
	logger      *logx.Logger
	monitor     *tg.Client
	adminBot    *tg.Client
	adminPoller *listener.Poller
	trigger     *service.TriggerService
	sender      *sender.Sender
	queue       *queue.MessageQueue
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

	blacklistStore, err := storage.NewBlacklistStore(cfg.BlacklistFile)
	if err != nil {
		return nil, err
	}

	settingsStore, err := storage.NewSettingsStore(cfg.SettingsFile, storage.RuntimeSettings{
		MonitoringEnabled: true,
		MonitorChatIDs:    mapKeys(cfg.MonitorChatIDs),
		AlertChatID:       cfg.AlertChatID,
		CooldownMinutes:   int(cfg.Cooldown / time.Minute),
		MaxMessageLength:  100,
		FilterNoUsername:  true,
		FilterNoAvatar:    false,
		MinAccountAgeDays: 7,
		DMTemplate:        cfg.DMTemplate,
		DryRun:            cfg.DryRun,
	})
	if err != nil {
		return nil, err
	}

	var adminClient *tg.Client
	var adminPoller *listener.Poller
	var adminAuth *tg.AdminAuth
	if cfg.BotToken != "" && cfg.AdminUserID != 0 {
		adminClient = tg.NewBotAPIClient(cfg.BotToken)
		adminAuth = tg.NewAdminAuth(cfg.Phone, cfg.AdminUserID, adminClient, logger)
		adminSvc := service.NewAdminService(cfg.AdminUserID, adminClient, keywordStore, settingsStore, blacklistStore, adminAuth, logger)
		adminPoller = listener.NewPoller(adminClient, cfg.PollTimeout, logger, func(ctx context.Context, update model.Update) error {
			_, err := adminSvc.HandleUpdate(ctx, update)
			return err
		})
	}
	monitorClient := tg.NewClient(cfg.AppID, cfg.AppHash, cfg.Phone, cfg.SessionFile, logger)
	if adminAuth != nil {
		monitorClient = tg.NewClientWithAuth(cfg.AppID, cfg.AppHash, cfg.Phone, cfg.SessionFile, adminAuth, logger)
	}

	jobQueue := queue.NewMessageQueue(cfg.QueueSize)
	m := matcher.NewKeywordMatcher()
	ruleEngine := rules.NewEngine(settingsStore, recordStore)
	dmSender := sender.New(monitorClient, recordStore, settingsStore, logger)
	triggerSvc := service.NewTriggerService(m, keywordStore, ruleEngine, jobQueue, recordStore, monitorClient, adminClient, settingsStore, blacklistStore, logger)

	return &App{
		cfg:         cfg,
		logger:      logger,
		monitor:     monitorClient,
		adminBot:    adminClient,
		adminPoller: adminPoller,
		trigger:     triggerSvc,
		sender:      dmSender,
		queue:       jobQueue,
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	a.logger.Infof("jtbot user session version started")
	if a.adminPoller != nil {
		a.logger.Infof("admin bot backend enabled")
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 2)
	var wg sync.WaitGroup

	go a.sender.Run(runCtx, a.queue.Consume())

	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- a.monitor.Run(runCtx, a.trigger.HandleUpdate)
	}()

	if a.adminPoller != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- a.adminPoller.Run(runCtx)
		}()
	}

	select {
	case <-ctx.Done():
		cancel()
		wg.Wait()
		return nil
	case err := <-errCh:
		cancel()
		if err == nil {
			wg.Wait()
			return nil
		}
		return err
	}
}

func mapKeys(input map[int64]struct{}) []int64 {
	result := make([]int64, 0, len(input))
	for id := range input {
		result = append(result, id)
	}
	return result
}
